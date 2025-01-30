using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Web;

namespace statsd.net.shared.Listeners
{
    public class AspNetCoreStatsListener : IListener
    {
        private readonly int _port;
        private readonly ISystemMetricsService _systemMetrics;
        private readonly ICorsValidationProvider _corsValidator;
        private ITargetBlock<string> _target;
        private IWebHost _webHost;
        private CancellationToken _cancellationToken;

        public AspNetCoreStatsListener(int port, ISystemMetricsService systemMetrics, ICorsValidationProvider corsValidator)
        {
            _port = port;
            _systemMetrics = systemMetrics;
            _corsValidator = corsValidator;
        }

        public bool IsListening { get; private set; }

        public void LinkTo(ITargetBlock<string> target, CancellationToken token)
        {
            _target = target;
            _cancellationToken = token;
            
            _webHost = new WebHostBuilder()
                .UseKestrel()
                .UseUrls($"http://*:{_port}")
                .Configure(app =>
                {
                    app.Use(async (context, next) =>
                    {
                        try
                        {
                            await HandleRequest(context);
                        }
                        catch (Exception)
                        {
                            _systemMetrics.LogCount("listeners.http.500");
                            await RespondWithStatus(context, "500 Internal Server Error");
                        }
                    });
                })
                .Build();

            Task.Run(async () =>
            {
                try
                {
                    IsListening = true;
                    await _webHost.RunAsync(_cancellationToken);
                }
                finally
                {
                    IsListening = false;
                }
            });
        }

        private async Task HandleRequest(HttpContext context)
        {
            var request = context.Request;
            var response = context.Response;

            if (request.Method == "OPTIONS")
            {
                await HandleOptionsRequest(context);
            }
            else if (request.Method == "POST")
            {
                await HandlePostRequest(context);
            }
            else if (request.Method == "GET" && request.Path == "/crossdomain.xml")
            {
                await HandleCrossDomainRequest(context);
            }
            else if (request.Method == "GET" && request.Path == "/clientaccesspolicy.xml")
            {
                await HandleClientAccessPolicyRequest(context);
            }
            else if (request.Method == "GET" && request.Query.ContainsKey("metrics"))
            {
                await HandleGetMetricsRequest(context);
            }
            else if (request.Method == "GET" && request.Path == "/")
            {
                await HandleLoadBalancerRequest(context);
            }
            else
            {
                _systemMetrics.LogCount("listeners.http.404");
                await RespondWithContent(context, "404 Not Found", "text/plain", "not found");
            }
        }

        private async Task HandleOptionsRequest(HttpContext context)
        {
            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    {"Content-Type", "text/plain"},
                    {"Content-Length", "0"}
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            await RespondWithStatus(context, "200 OK");
        }

        private async Task HandlePostRequest(HttpContext context)
        {
            using var reader = new StreamReader(context.Request.Body);
            var payload = await reader.ReadToEndAsync();

            try
            {
                _systemMetrics.LogCount("listeners.http.bytes", Encoding.UTF8.GetByteCount(payload));
                var lines = payload.Replace("\r", "")
                    .Split(new char[] { '\n', ',' }, StringSplitOptions.RemoveEmptyEntries);

                foreach (var line in lines)
                {
                    _target.Post(line);
                }
                
                _systemMetrics.LogCount("listeners.http.lines", lines.Length);
                await RespondWithStatus(context, "200 OK");
            }
            catch
            {
                await RespondWithStatus(context, "400 Bad Request");
            }
        }

        private async Task HandleCrossDomainRequest(HttpContext context)
        {
            var content = _corsValidator.GetFlashCrossDomainPolicy();
            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    { "Content-Type", "application/xml" },
                    { "Content-Length", Encoding.UTF8.GetByteCount(content).ToString() }
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            await RespondWithContent(context, "200 OK", "application/xml", content);
        }

        private async Task HandleClientAccessPolicyRequest(HttpContext context)
        {
            var content = _corsValidator.GetSilverlightCrossDomainPolicy();
            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    { "Content-Type", "text/xml" },
                    { "Content-Length", Encoding.UTF8.GetByteCount(content).ToString() }
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            await RespondWithContent(context, "200 OK", "text/xml", content);
        }

        private async Task HandleGetMetricsRequest(HttpContext context)
        {
            var metrics = context.Request.Query["metrics"].ToString();
            var lines = metrics.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            
            foreach (var line in lines)
            {
                _target.Post(line);
            }

            _systemMetrics.LogCount("listeners.http.lines", lines.Length);
            _systemMetrics.LogCount("listeners.http.bytes", Encoding.UTF8.GetByteCount(metrics));

            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    { "Content-Type", "application/xml" },
                    { "Content-Length", "0" }
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            await RespondWithStatus(context, "200 OK");
        }

        private async Task HandleLoadBalancerRequest(HttpContext context)
        {
            _systemMetrics.LogCount("listeners.http.loadbalancer");
            await RespondWithStatus(context, "200 OK");
        }

        private async Task RespondWithStatus(HttpContext context, string status)
        {
            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    { "Content-Type", "text/plain" },
                    { "Content-Length", "0" }
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            context.Response.StatusCode = int.Parse(status.Split(' ')[0]);
        }

        private async Task RespondWithContent(HttpContext context, string status, string contentType, string content)
        {
            var corsHeaders = _corsValidator.AppendCorsHeaderDictionary(
                ConvertToKayakRequestHead(context.Request),
                new Dictionary<string, string>
                {
                    { "Content-Type", contentType },
                    { "Content-Length", Encoding.UTF8.GetByteCount(content).ToString() }
                });

            await AddCorsHeaders(context.Response, corsHeaders);
            context.Response.StatusCode = int.Parse(status.Split(' ')[0]);
            await context.Response.WriteAsync(content);
        }

        private async Task AddCorsHeaders(HttpResponse response, Dictionary<string, string> headers)
        {
            foreach (var header in headers)
            {
                response.Headers[header.Key] = header.Value;
            }
        }

        private HttpRequestHead ConvertToKayakRequestHead(HttpRequest request)
        {
            return new HttpRequestHead
            {
                Method = request.Method,
                Uri = request.Path,
                Headers = request.Headers.ToDictionary(h => h.Key, h => h.Value.ToString()),
                QueryString = request.QueryString.ToString()
            };
        }
    }

    public class HttpRequestHead
    {
        public string Method { get; set; }
        public string Uri { get; set; }
        public Dictionary<string, string> Headers { get; set; }
        public string QueryString { get; set; }
    }
}
