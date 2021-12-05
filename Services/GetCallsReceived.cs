using PullTelax.Shared;
using PullTelax.Services.Interfaces;
using PullTelax.Repo;
using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PullTelax.Services
{
    public class GetCallsReceived : IGetCallsReceived
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<GetCallsReceived> _logger;
        private readonly ISharedLibs _shared;
        private readonly JsonSerializerOptions _options = new()
        {
            PropertyNameCaseInsensitive = true
        };

        private const string ApiUrl = "https://pop1-apps.mycontactcenter.net/api/v2/hist/incomingvoicecalls/"; ///v2/hist/incomingvoicecalls/{date}

        public GetCallsReceived(IHttpClientFactory httpClientFactory, ILogger<GetCallsReceived> logger, ISharedLibs shared)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _shared = shared;
        }

        public async Task<string> GetCallsReceivedAsync()
        {
            try
            {
                int rowsinserted = 0;
                int rowsaffected = 0;
                var loop_counter = 0;
                var jsonDocs = "";
                var _httpClient = _httpClientFactory.CreateClient();

                _httpClient.DefaultRequestHeaders.Add("token", "HzF1ZfzvROGH3X9FJoxW6oeN45G3+yPIYcEA7zb6P74=");

                loop_counter = await _shared.GetTotalPages(ApiUrl + DateTime.Now.ToString("yyyy-MM-dd"));
                for (int i = 1; i <= loop_counter; i++)
                {
                    _httpClient.DefaultRequestHeaders.Add("page", i.ToString());
                    jsonDocs = await _httpClient.GetStringAsync(ApiUrl + DateTime.Now.ToString("yyyy-MM-dd"));

                    PostCallCenterData callRepo = new PostCallCenterData();
                    rowsinserted = callRepo.InsertRows(jsonDocs, "import.usp_Import_Calls_Received");
                    rowsaffected += rowsinserted;
                    _httpClient.DefaultRequestHeaders.Remove("page");
                };

                return jsonDocs is null
                    ? $"No datat Pulled for Calls Received "
                    : $"Calls Received: {rowsaffected} rows inserted";
            }
            catch (HttpRequestException httpEX)
            {
                _logger.LogError(httpEX.Message);
                return $"Http Status Code {httpEX.StatusCode}";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                return $"That's not funny! {ex}";
            }
        }
        //private async Task<int> GetPages()
        //{
        //    int totalPages = 0;
        //    var request = new HttpRequestMessage(HttpMethod.Get, ApiUrl + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd"));
        //    request.Headers.Add("token", "HzF1ZfzvROGH3X9FJoxW6oeN45G3+yPIYcEA7zb6P74=");

        //    var client = _httpClientFactory.CreateClient();

        //    try
        //    {
        //        var response = await client.SendAsync(request);
        //        if (response.IsSuccessStatusCode)
        //        {
        //            var resp_headers = response.Headers;
        //            var PageHeaders = resp_headers.GetValues("pagination");
        //            foreach (var header in PageHeaders)
        //            {
        //                pageHeaders pages = JsonSerializer.Deserialize<pageHeaders>(header.ToString());
        //                totalPages = pages.TotalPages;
        //            }

        //            return totalPages;
        //        }
        //        else
        //        {
        //            return 0;
        //            //GetBranchesError = true;
        //            //Branches = Array.Empty<GitHubBranch>();
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex.Message);
        //        return 0;
        //    }
        //}
    }
    //PageNumber":1,"TotalPages":17}
    //public record pageHeaders(int PageNumber, int TotalPages);
}
