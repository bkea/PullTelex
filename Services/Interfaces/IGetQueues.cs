using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetQueues
    {
        public Task<string> GetQueuesAsync();
    }
}
