using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PullTelax.Services.Interfaces;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace PullTelax
{
    public class Worker : BackgroundService
    {
        private readonly IGetAgents _agentService;
        private readonly IGetAgentSessions _agentSession;
        private readonly IGetAgentStatus _agentStatus;
        private readonly IGetQueues _agentQueue;
        private readonly IGetCallsMade _callsMade;
        private readonly IGetCallsReceived _callsReceived;
        private readonly ILogger<Worker> _logger;

        public Worker(
            IGetAgents agentService,
            IGetAgentSessions agentSession,
            IGetAgentStatus agentStatus,
            IGetQueues agentQueue,
            IGetCallsMade callsMade,
            IGetCallsReceived callsReceived,
            ILogger<Worker> logger)
        {
            _agentService = agentService;
            _agentSession = agentSession;
            _agentStatus = agentStatus;
            _agentQueue = agentQueue;
            _callsMade = callsMade;
            _callsReceived = callsReceived;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                try
                {
                    string agent = await _agentService.GetAgentAsync();
                    _logger.LogWarning(agent);

                    string queue = await _agentQueue.GetQueuesAsync();
                    _logger.LogWarning(queue);

                    string agentSession = await _agentSession.GetAgentSessionsAsync();
                    _logger.LogWarning(agentSession);

                    string agentStatus = await _agentStatus.GetAgentStatusAsync();
                    _logger.LogWarning(agentStatus);

                    string incommingCalls = await _callsReceived.GetCallsReceivedAsync();
                    _logger.LogWarning(incommingCalls);

                    string outgoingCalls = await _callsMade.GetCallsMadeAsync();
                    _logger.LogWarning(outgoingCalls);

                    //await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                    await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
                   // await Task.Delay(3000, stoppingToken);
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}
