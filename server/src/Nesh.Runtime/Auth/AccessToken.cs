using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Storage.Models;
using Nesh.Abstractions.Utils;
using Orleans;
using System;
using System.Threading.Tasks;

namespace Nesh.Runtime.Auth
{
    public class AccessToken : Grain, IAccessToken
    {
        private string _Token { get; set; }
        private ILogger _Logger { get; set; }
        private IAccountDB AccountDB { get; set; }

        public override Task OnActivateAsync()
        {
            _Token = this.GetPrimaryKeyString();
            _Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger($"{nameof(AccessToken)} [{_Token}]");

            AccountDB = ServiceProvider.GetService<IAccountDB>();

            return base.OnActivateAsync();
        }

        public async Task<Guid> GetUserId()
        {
            Account account = await AccountDB.GetByToken(_Token);

            if (account == null)
            {
                _Logger.LogError($"access failed for token {_Token}");
            }

            return account != null ? account.user_id : Guid.Empty;
        }

        public async Task<bool> IsExpired()
        {
            Account account = await AccountDB.GetByToken(_Token);
            if (account == null) return true;

            return account.expired_time <= TimeUtils.Now;
        }
    }
}