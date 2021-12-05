using System.Threading.Tasks;

namespace PullTelax.Shared
{
    public interface ISharedLibs
    {
        public Task<int> GetTotalPages(string ApiUrl);
    }
}
