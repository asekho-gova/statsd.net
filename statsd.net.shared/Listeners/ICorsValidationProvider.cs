using System.Collections.Generic;
using Microsoft.AspNetCore.Http;

namespace statsd.net.shared.Listeners
{
  public interface ICorsValidationProvider
  {
    Dictionary<string, string> AppendCorsHeaderDictionary(HttpRequest request, Dictionary<string, string> headers);
    string GetFlashCrossDomainPolicy();
    string GetSilverlightCrossDomainPolicy();
  }
}
