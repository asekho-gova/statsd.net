using System.ComponentModel.Composition;
using System.Xml.Linq;
using log4net;
using Microsoft.Practices.TransientFaultHandling;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using statsd.net.Configuration;
using statsd.net.core;
using statsd.net.core.Backends;
using statsd.net.core.Structures;
using statsd.net.shared;
using statsd.net.shared.Messages;
using statsd.net.shared.Services;
using statsd.net.shared.Structures;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace statsd.net.Backends.Librato
{
  /**
   * Flow of data:
   *  
   *   Bucket ->
   *      Preprocessor ->
   *         Batch Block ->
   *            Post to Librato 
   */
  [Export(typeof(IBackend))]
  public class LibratoBackend : IBackend
  {
    public const string ILLEGAL_NAME_CHARACTERS = @"[^-.:_\w]+";
    public const string LIBRATO_API_URL = "https://metrics-api.librato.com";

    private Task _completionTask;
    private ILog _log;
    private string _serviceVersion;
    public bool IsActive { get; private set; }
    private ActionBlock<Bucket> _preprocessorBlock;
    private BatchBlock<LibratoMetric> _batchBlock;
    private ActionBlock<LibratoMetric[]> _outputBlock;
    private HttpClient _client;
    private ISystemMetricsService _systemMetrics;
    private int _pendingOutputCount;
    private RetryPolicy<LibratoErrorDetectionStrategy> _retryPolicy;
    private Incremental _retryStrategy;
    private LibratoBackendConfiguration _config;
    private string _source;

    public int OutputCount
    {
      get { return _pendingOutputCount; }
    }

    public string Name { get { return "Librato"; } }  
    
    public void Configure(string collectorName, XElement configElement, ISystemMetricsService systemMetrics)
    {
      _completionTask = new Task(() => IsActive = false);
      _log = SuperCheapIOC.Resolve<ILog>();
      _systemMetrics = systemMetrics;

      var config = new LibratoBackendConfiguration(
          email: configElement.Attribute("email").Value,
          token: configElement.Attribute("token").Value,
          numRetries: configElement.ToInt("numRetries"),
          retryDelay: Utility.ConvertToTimespan(configElement.Attribute("retryDelay").Value),
          postTimeout: Utility.ConvertToTimespan(configElement.Attribute("postTimeout").Value),
          maxBatchSize: configElement.ToInt("maxBatchSize"),
          countersAsGauges: configElement.ToBoolean("countersAsGauges")
        );
      
      _config = config;
      _source = collectorName;
      _serviceVersion = Assembly.GetEntryAssembly().GetName().Version.ToString();

      _preprocessorBlock = new ActionBlock<Bucket>(bucket => ProcessBucket(bucket), Utility.UnboundedExecution());
      _batchBlock = new BatchBlock<LibratoMetric>(_config.MaxBatchSize);
      _outputBlock = new ActionBlock<LibratoMetric[]>(lines => PostToLibrato(lines), Utility.OneAtATimeExecution());
      _batchBlock.LinkTo(_outputBlock);

      _client = new HttpClient();
      _client.BaseAddress = new Uri(LIBRATO_API_URL);
      _client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(
          "Basic", 
          Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes($"{_config.Email}:{_config.Token}"))
      );
      _client.DefaultRequestHeaders.Add("User-Agent", "statsd.net-librato-backend/" + _serviceVersion);
      _client.Timeout = _config.PostTimeout;

      _retryPolicy = new RetryPolicy<LibratoErrorDetectionStrategy>(_config.NumRetries);
      _retryPolicy.Retrying += (sender, args) =>
      {
        _log.Warn(String.Format("Retry {0} failed. Trying again. Delay {1}, Error: {2}", args.CurrentRetryCount, args.Delay, args.LastException.Message), args.LastException);
        _systemMetrics.LogCount("backends.librato.retry");
      };
      _retryStrategy = new Incremental(_config.NumRetries, _config.RetryDelay, TimeSpan.FromSeconds(2));
      IsActive = true;
    }

    public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader,
      Bucket messageValue,
      ISourceBlock<Bucket> source,
      bool consumeToAccept)
    {
      _preprocessorBlock.Post(messageValue);
      return DataflowMessageStatus.Accepted;
    }

    public void Complete()
    {
      _completionTask.Start();
    }

    public Task Completion
    {
      get { return _completionTask; }
    }

    public void Fault(Exception exception)
    {
      throw new NotImplementedException();
    }

    private void ProcessBucket(Bucket bucket)
    {
      switch (bucket.BucketType)
      {
        case BucketType.Count:
          var counterBucket = bucket as CounterBucket;
          foreach (var count in counterBucket.Items)
          {
            if (_config.CountersAsGauges)
            {
              _batchBlock.Post(new LibratoGauge(counterBucket.RootNamespace + count.Key, count.Value, bucket.Epoch));
            }
            else
            {
              _batchBlock.Post(new LibratoCounter(counterBucket.RootNamespace + count.Key, count.Value, bucket.Epoch));
            }
          }
          break;
        case BucketType.Gauge:
          var gaugeBucket = bucket as GaugesBucket;
          foreach (var gauge in gaugeBucket.Gauges)
          {
            _batchBlock.Post(new LibratoGauge(gaugeBucket.RootNamespace + gauge.Key, gauge.Value, bucket.Epoch));
          }
          break;
        case BucketType.Timing:
          var timingBucket = bucket as LatencyBucket;
          foreach (var timing in timingBucket.Latencies)
          {
            _batchBlock.Post(new LibratoTiming(timingBucket.RootNamespace + timing.Key,
              timing.Value.Count,
              timing.Value.Sum,
              timing.Value.SumSquares,
              timing.Value.Min,
              timing.Value.Max,
              bucket.Epoch));
          }
          break;
        case BucketType.Percentile:
          var percentileBucket = bucket as PercentileBucket;
          double percentileValue;
          foreach (var pair in percentileBucket.Timings)
          {
            if (percentileBucket.TryComputePercentile(pair, out percentileValue))
            {
              _batchBlock.Post(new LibratoGauge(percentileBucket.RootNamespace + pair.Key + percentileBucket.PercentileName,
                percentileValue,
                bucket.Epoch));
            }
          }
          break;
      }
    }

    private void PostToLibrato(LibratoMetric[] lines)
    {
      try
      {
        PostToLibratoInternal(lines);
      }
      catch (Exception ex)
      {
        _log.Error("Failed to post metrics to Librato.com", ex);
        _systemMetrics.LogCount("backends.librato.post.error." + ex.GetType().Name);
      }
    }

    private void PostToLibratoInternal(LibratoMetric[] lines)
    {
      var pendingLines = 0;
      foreach (var epochGroup in lines.GroupBy(p => p.Epoch))
      {
        var payload = GetPayload(epochGroup);
        pendingLines = payload.gauges.Length + payload.counters.Length;
        _systemMetrics.LogGauge("backends.librato.lines", pendingLines);
        Interlocked.Add(ref _pendingOutputCount, pendingLines);

        _retryPolicy.ExecuteAction(async () =>
          {
            bool succeeded = false;
            try
            {
              _systemMetrics.LogCount("backends.librato.post.attempt");
              var response = await _client.PostAsJsonAsync("/v1/metrics", payload);
              
              if (response.StatusCode == HttpStatusCode.Unauthorized)
              {
                _systemMetrics.LogCount("backends.librato.error.unauthorised");
                throw new UnauthorizedAccessException("Librato.com reports that your access is not authorised. Is your API key and email address correct?");
              }
              else if (!response.IsSuccessStatusCode)
              {
                _systemMetrics.LogCount("backends.librato.error." + response.StatusCode.ToString());
                throw new Exception($"Request could not be processed. Server said {response.StatusCode}");
              }
              else
              {
                succeeded = true;
                _log.Info($"Wrote {pendingLines} lines to Librato.");
              }
            }
            finally
            {
              Interlocked.Add(ref _pendingOutputCount, -pendingLines);
              _systemMetrics.LogCount("backends.librato.post." + (succeeded ? "success" : "failure"));
            }
          });
      }
    }

    private APIPayload GetPayload(IGrouping<long, LibratoMetric> epochGroup)
    {
      var lines = epochGroup.ToList();
      // Split the lines up into gauges and counters
      var gauges = lines.Where(p => p.MetricType == LibratoMetricType.Gauge || p.MetricType == LibratoMetricType.Timing).ToArray();
      var counts = lines.Where(p => p.MetricType == LibratoMetricType.Counter).ToArray();

      var payload = new APIPayload();
      payload.gauges = gauges;
      payload.counters = counts;
      payload.measure_time = epochGroup.Key;
      payload.source = _source;
      return payload;
    }

    private class LibratoErrorDetectionStrategy : ITransientErrorDetectionStrategy
    {
      public bool IsTransient(Exception ex)
      {
        if (ex is TimeoutException)
        {
          return true;
        }
        return false;
      }
    }
  }
}
