using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetAgentSessions
    {
        public Task<string> GetAgentSessionsAsync();
    }
}
