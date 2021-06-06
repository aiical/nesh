using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Storage.Models;
using Nesh.Abstractions.Utils;
using System;
using System.Threading.Tasks;

namespace Nesh.Runtime.Database
{
    public class AccountDB : IAccountDB
    {
        private IMongoDatabase Database { get; }
        private IMongoCollection<Account> Collection;
        private IMongoClient MongoClient { get; }
        private ILogger Logger { get; }

        private const string DATABASE = "auth";
        private const string COLLECTION = "account";

        public AccountDB(ILogger<AccountDB> logger, IMongoClient mongo)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            MongoClient = mongo ?? throw new ArgumentNullException(nameof(mongo));
            Database = MongoClient.GetDatabase(DATABASE);
            Collection = Database.GetCollection<Account>(COLLECTION);
        }

        public async Task<Account> GetByToken(string access_token)
        {
            try
            {
                var filter = Builders<Account>.Filter.Eq(nameof(Account.access_token), access_token);

                IAsyncCursor<Account> result = await Collection.FindAsync(filter);
                Account account = await result.FirstOrDefaultAsync();

                return account;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "GetByToken Failed");
            }

            return null;
        }

        public async Task<Account> GetByPlatformId(string platform_id)
        {
            var filter = Builders<Account>.Filter.Eq(nameof(Account.platform_id), platform_id);

            IAsyncCursor<Account> result = await Collection.FindAsync(filter);
            Account account = await result.FirstOrDefaultAsync();

            return account;
        }

        public async Task<Account> CreateAccount(Platform platform, string platform_id)
        {
            Account account = new Account();
            account.platform = platform;
            account.platform_id = platform_id;
            account.user_id = Guid.NewGuid();
            account.hash_slat = BCrypt.Net.BCrypt.GenerateSalt();
            account.create_time = TimeUtils.Now;
            await Collection.InsertOneAsync(account);

            return account;
        }

        public async Task RefreshToken(Guid user_id, string access_token)
        {
            try
            {
                var filter = Builders<Account>.Filter.Eq(nameof(Account.user_id), user_id);
                var update = Builders<Account>.Update.Set(nameof(Account.access_token), access_token);
                await Collection.UpdateOneAsync(filter, update);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "RefreshToken Failed");
            }
        }

        public async Task<Account> GetByUserId(Guid user_id)
        {
            try
            {
                var filter = Builders<Account>.Filter.Eq(nameof(Account.user_id), user_id);

                IAsyncCursor<Account> result = await Collection.FindAsync(filter);
                return await result.FirstOrDefaultAsync();
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "GetByUserId Failed");
            }

            return null;
        }
    }
}
