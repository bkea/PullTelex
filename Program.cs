using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PullTelax.Services;
using PullTelax.Services.Interfaces;
using PullTelax.Shared;

namespace PullTelax
{
    public class Program
    {
        public Program(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>();
                    services.AddHttpClient();
                    services.AddSingleton<IGetAgents, GetAgents>();
                    services.AddSingleton<IGetAgentSessions, GetAgentSessions>();
                    services.AddSingleton<IGetAgentStatus, GetAgentStatus>();
                    services.AddSingleton<IGetQueues, GetQueues>();
                    services.AddSingleton<IGetCallsReceived, GetCallsReceived>();
                    services.AddSingleton<IGetCallsMade, GetCallsMade>();
                    services.AddSingleton<ISharedLibs, SharedLibs>();
                });
    }
}