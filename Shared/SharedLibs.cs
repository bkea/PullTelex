using Microsoft.Extensions.Logging;
using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

namespace PullTelax.Shared
{
    class SharedLibs : ISharedLibs
    {

        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<SharedLibs> _logger;
        private readonly JsonSerializerOptions _options = new()
        {
            PropertyNameCaseInsensitive = true
        };
        public SharedLibs(IHttpClientFactory httpClientFactory, ILogger<SharedLibs> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }
        public async Task<int> GetTotalPages(string ApiUrl)
        {
            int totalPages = 0;
            var request = new HttpRequestMessage(HttpMethod.Get, ApiUrl);
            request.Headers.Add("token", "HzF1ZfzvROGH3X9FJoxW6oeN45G3+yPIYcEA7zb6P74=");

            var client = _httpClientFactory.CreateClient();

            try
            {
                var response = await client.SendAsync(request);
                if (response.IsSuccessStatusCode)
                {
                    var resp_headers = response.Headers;

                    System.Collections.Generic.IEnumerable<string>  PageHeaders;
                    if (resp_headers.TryGetValues("pagination", out PageHeaders))
                    {
                        //var PageHeaders = resp_headers.GetValues("pagination");
                        foreach (var header in PageHeaders)
                        {
                            pageHeaders pages = JsonSerializer.Deserialize<pageHeaders>(header.ToString());
                            totalPages = pages.TotalPages;
                        }
                    }
                    return totalPages;
                }
                else
                {
                    return 0;

                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                return 0;
            }
        }
    }
    //PageNumber":1,"TotalPages":17}
    public record pageHeaders(int PageNumber, int TotalPages);
}
