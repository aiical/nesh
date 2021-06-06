using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Storage.Models;
using Nesh.Runtime.Service;
using Orleans;
using System;
using System.Threading.Tasks;

namespace Nesh.Runtime.Auth
{
    public class PlatformSession : Grain, IPlatformSession
    {
        private string PlatformId { get; set; }
        private ILogger _Logger { get; set; }
        private IAccountDB AccountDB { get; set; }
        private bool Verified { get; set; }

        public override Task OnActivateAsync()
        {
            PlatformId = this.GetPrimaryKeyString();
            _Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger($"{nameof(PlatformSession)} [{PlatformId}]");
            Verified = false;
            AccountDB = ServiceProvider.GetService<IAccountDB>();

            return base.OnActivateAsync();
        }

        public async Task RefreshToken(string access_token)
        {
            try
            {
                Account account = await AccountDB.GetByPlatformId(PlatformId);
                if (account == null)
                {
                    throw new Exception($"{PlatformId} is exist by access_token={access_token}");
                }

                await AccountDB.RefreshToken(account.user_id, access_token);
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "RefreshToken Failed");
            }
        }

        public async Task<Account> VerifyAccount(Platform platform, string platform_id)
        {
            Account account = await AccountDB.GetByPlatformId(platform_id);
            if (account == null)
            {
                account = await AccountDB.CreateAccount(platform, platform_id);
            }

            return account;
        }
    }
}
