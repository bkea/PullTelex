using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetCallsReceived
    {
        public Task<string> GetCallsReceivedAsync();
    }
}
