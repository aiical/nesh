using Microsoft.Extensions.Logging;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using Nesh.Runtime.Utils;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    public partial class Node
    {
        public async Task<bool> Exists(Nuid id)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.Exists(id);
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);

                return entity != null;
            }
            else if (NodeType == NodeType.Cache)
            {
                return await CacheExist(id);
            }

            return false;
        }

        public async Task<string> GetType(Nuid id)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetType(id);
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} GetType when Grain dont found entity!");
                    return Global.NULL_STRING;
                }

                return entity.Type;
            }
            else if (NodeType == NodeType.Cache)
            {
                return await GetCacheType(id);
            }

            return Global.NULL_STRING;
        }

        public async Task<Nuid> Create(Nuid id, string type, NList args)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.Create(id, type, args);
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Create(id, type);
                if (entity == null)
                {
                    _Logger.LogError($"{id} Create Entity Failed when {id} {type}!");
                    return Nuid.Empty;
                }
            }
            else if (NodeType == NodeType.Cache)
            {
                if (!await SetCacheType(id, type))
                {
                    _Logger.LogError($"{id} Create Entity when SetCacheType Failed {id} {type}!");
                    return Nuid.Empty;
                }
            }

            await CallbackEntity(id, EntityEvent.OnCreate, args);

            return id;
        }

        public async Task<Nuid> Create(string type, Nuid origin, NList args)
        {
            long unique = IdGenerator.NewIdentity();
            Nuid id = Nuid.New(unique, origin.Origin);
            return await Create(id, type, args);
        }

        public async Task Entry(Nuid id)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                if (await node.IsActive())
                {
                    await node.Entry(id);
                    return;
                }
            }

            Entity entity = null;
            if (NodeType == NodeType.Grain)
            {
                entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} Entry Entity Failed when {id} not found!");
                    return;
                }
            }
            else if (NodeType == NodeType.Cache)
            {
                if (!await CacheExist(id))
                {
                    _Logger.LogError($"{id} Entry Entity Failed when not CacheExist!");
                    return;
                }

                entity = await GetCacheEntity(id);
            }

            await CallbackEntity(id, EntityEvent.OnEntry, NList.Empty);

            await SyncEntity(id, NList.New().Add(id).Add((int)EntityEvent.OnEntry).Add(entity));
        }

        private async Task Load(Entity entity)
        {
            if (entity == null) return;

            if (EntityManager.Find(entity.Id))
            {
                EntityManager.Remove(entity.Id);
            }

            EntityManager.Add(entity);
            await CallbackEntity(entity.Id, EntityEvent.OnLoad, NList.Empty);
        }

        public async Task Leave(Nuid id)
        {
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                await CallbackEntity(id, EntityEvent.OnLeave, NList.Empty);

                await SyncEntity(id, NList.New().Add(id).Add((int)EntityEvent.OnLeave));
            }
            else
            {
                if (id.Origin == Identity) return;

                INode node = GrainFactory.GetGrain<INode>(id.Origin);

                if (await node.IsActive())
                {
                    await node.Leave(id);
                }
            }
        }

        public async Task Destroy(Nuid id)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                if (await node.IsActive())
                {
                    await node.Destroy(id);
                    return;
                }
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} Destroy Entity Failed When not found!");
                    return;
                }

                if (EntityManager.Remove(id))
                {
                    _Logger.LogError($"{id} Destroy Entity Failed When EntityManager Remove!");
                    return;
                }
            }
            else if (NodeType == NodeType.Cache)
            {
                if (!await CacheExist(id))
                {
                    _Logger.LogError($"{id} Destroy Entity Failed when not CacheExist!");
                    return;
                }

                string entity_type = await GetCacheType(id);
                EntityPrefab entity_prefab = Prefabs.GetEntity(entity_type);
                if (entity_prefab == null)
                {
                    _Logger.LogError($"{id} Destroy Entity Failed when not EntityPrefab {entity_type}!");
                    return;
                }

                int db = (int)(Identity % CacheUtils.EntityDBs);
                IRedisDatabase redis = _CacheClient.GetDb(db);

                ITransaction trans = redis.Database.CreateTransaction();

                foreach (TablePrefab table_prefab in entity_prefab.tables.Values)
                {
                    string table_key = CacheUtils.BuildTable(id, table_prefab.name);
                    Task table_task = trans.KeyDeleteAsync(table_key);
                }

                string field_key = CacheUtils.BuildFields(id);
                Task field_task = trans.KeyDeleteAsync(field_key);

                string entity_key = CacheUtils.BuildEntities(id);
                Task entity_task = trans.HashDeleteAsync(entity_key, id.Unique);

                bool result = await trans.ExecuteAsync();

                if (!result)
                {
                    _Logger.LogError($"{id} Destroy Entity Failed when ITransaction Execute");
                    return;
                }
            }

            await CallbackEntity(id, EntityEvent.OnDestroy, NList.Empty);

            await SyncEntity(id, NList.New().Add(id).Add((int)EntityEvent.OnDestroy));
        }

        public async Task<NList> GetEntities()
        {
            NList list = NList.New();

            if (NodeType == NodeType.Grain)
            {
                IReadOnlyList<Entity> entities = EntityManager.GetEntities();
                foreach (Entity entity in entities)
                {
                    list.Add(entity);
                }
            }
            else if (NodeType == NodeType.Cache)
            {
                IReadOnlyList<Entity> entities = await GetCacheEntities();
                foreach (Entity entity in entities)
                {
                    list.Add(entity);
                }
            }

            return list;
        }
    }
}
