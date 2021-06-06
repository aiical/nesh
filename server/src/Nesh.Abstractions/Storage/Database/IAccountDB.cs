using Nesh.Abstractions.Storage.Models;
using System;
using System.Threading.Tasks;

namespace Nesh.Abstractions.Storage.Database
{
    public interface IAccountDB
    {
        Task<Account> GetByToken(string access_token);

        Task<Account> GetByPlatformId(string platform_id);

        Task<Account> CreateAccount(Platform platform, string platform_id);

        Task RefreshToken(Guid user_id, string access_token);

        Task<Account> GetByUserId(Guid user_id);
    }
}
