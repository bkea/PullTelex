using System.Threading.Tasks;

namespace PullTelax.Services.Interfaces
{
    public interface IGetCallsMade
    {
        public Task<string> GetCallsMadeAsync();
    }
}
