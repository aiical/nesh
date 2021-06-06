using Nesh.Abstractions.Storage.Models;
using Orleans;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Auth
{
    public interface IPlatformSession : IGrainWithStringKey
    {
        Task<Account> VerifyAccount(Platform platform, string unionid);
        Task RefreshToken(string access_token);
    }
}
