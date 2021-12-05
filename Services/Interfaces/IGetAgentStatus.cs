using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetAgentStatus
    {
        public Task<string> GetAgentStatusAsync();
    }
}
