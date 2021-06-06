using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Storage.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Database
{
    public class EntityDB : IEntityDB
    {
        protected IMongoDatabase Database { get; }
        private IMongoClient MongoClient { get; }
        private ILogger Logger { get; }
        private IMongoCollection<EntityList> Collection { get; }

        public EntityDB(ILogger<IEntityDB> logger, IMongoClient mongo)
        {
            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
            MongoClient = mongo ?? throw new ArgumentNullException(nameof(mongo));
            Database = MongoClient.GetDatabase("entity");
            Collection = Database.GetCollection<EntityList>("entities");
        }

        public async Task<bool> IsPersist(long origin)
        {
            try
            {
                var filter = Builders<EntityList>.Filter.Eq(nameof(EntityList.origin), origin);
                IAsyncCursor<EntityList> cursor = await Collection.FindAsync(filter);
                var list = await cursor.ToListAsync();

                return list != null && list.Count > 0;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"IsPersist Failed! {origin}");
            }

            return false;
        }

        public async Task<IReadOnlyList<Entity>> GetEntities(long origin)
        {
            try
            {
                var filter = Builders<EntityList>.Filter.Eq(nameof(EntityList.origin), origin);
                IAsyncCursor<EntityList> cursor = await Collection.FindAsync(filter);

                var list = await cursor.ToListAsync();

                List<Entity> entities = new List<Entity>();

                return entities;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, $"GetEntities Failed origin={origin}");
            }

            return new List<Entity>();
        }

        public async Task<NodeType> GetNodeType(long origin)
        {
            try
            {
                var filter = Builders<EntityList>.Filter.Eq(nameof(EntityList.origin), origin);
                IAsyncCursor<EntityList> cursor = await Collection.FindAsync(filter);

                var entity_list = await cursor.SingleOrDefaultAsync();

                return entity_list.node;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "GetNodeType Failed");
            }

            return NodeType.None;
        }

        public async Task SetNodeType(long origin, NodeType node_type)
        {
            try
            {
                if (node_type == NodeType.None)
                {
                    throw new Exception($"origin {origin} set NodeType.None");
                }

                var filter = Builders<EntityList>.Filter.Eq(nameof(EntityList.origin), origin);
                IAsyncCursor<EntityList> cursor = await Collection.FindAsync(filter);

                if (await cursor.AnyAsync())
                {
                    return;
                }

                EntityList entities = new EntityList();
                entities.node = node_type;
                entities.origin = origin;
                entities.entities = new List<EntityChild>();
                await Collection.InsertOneAsync(entities);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "SetNodeType Failed");
            }
        }
    }
}