using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Storage.Models;
using System;
using System.Threading.Tasks;

namespace Nesh.Runtime.Database
{
    public class RealmDB : IRealmDB
    {
        private IMongoDatabase Database { get; }
        private IMongoCollection<Realm> Collection;
        private IMongoClient MongoClient { get; }
        private ILogger Logger { get; }

        private const string DATABASE = "auth";
        private const string COLLECTION = "realm";

        public RealmDB(ILogger<RealmDB> logger, IMongoClient mongo)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            MongoClient = mongo ?? throw new ArgumentNullException(nameof(mongo));
            Database = MongoClient.GetDatabase(DATABASE);
            Collection = Database.GetCollection<Realm>(COLLECTION);
        }

        public async Task<Realm> GetRealm(int realm_id)
        {
            try
            {
                var filter = Builders<Realm>.Filter.Eq(nameof(Realm.id), realm_id);

                IAsyncCursor<Realm> result = await Collection.FindAsync(filter);
                Realm realm = await result.FirstOrDefaultAsync();
                return realm;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"GetByUserId Failed realm_id={realm_id}");
            }

            return null;
        }
    }
}