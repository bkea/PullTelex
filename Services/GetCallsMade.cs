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
    public class GetCallsMade : IGetCallsMade
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<GetCallsMade> _logger;
        private readonly ISharedLibs _shared;
        private readonly JsonSerializerOptions _options = new()
        {
            PropertyNameCaseInsensitive = true
        };

        private const string ApiUrl = "https://pop1-apps.mycontactcenter.net/api/v2/hist/dialoutvoicecalls/"; ///v2/hist/dialoutvoicecalls/{date}

        public GetCallsMade(IHttpClientFactory httpClientFactory, ILogger<GetCallsMade> logger, ISharedLibs shared)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _shared = shared;
        }

        public async Task<string> GetCallsMadeAsync()
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
                    rowsinserted = callRepo.InsertRows(jsonDocs, "Import.usp_Import_Calls_Made");
                    rowsaffected += rowsinserted;
                    _httpClient.DefaultRequestHeaders.Remove("page");
                }

                return jsonDocs is null
                    ? $"No datat Pulled for Calls Made "
                    : $"Calls Made: {rowsaffected} rows inserted";
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
    }
}