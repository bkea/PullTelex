using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetAgents
    {
        public Task<string> GetAgentAsync();
    }
}
