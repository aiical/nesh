using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using Nesh.Runtime.Utils;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    enum CacheOption : int
    {
        SetEntity = 1,
        DelEntity = 2,
        SetField = 3,
        ClearTable = 4,
        DelRow = 5,
        SetRow = 6,
    }

    public partial class Node
    {
        private IRedisDatabase GetCache()
        {
            int db = (int)(Identity % CacheUtils.EntityDBs);
            return _CacheClient.GetDb(db);
        }

        private IRedisDatabase GetCache(Nuid id)
        {
            int db = (int)(id.Origin % CacheUtils.EntityDBs);
            return _CacheClient.GetDb(db);
        }

        private async Task<bool> CacheExist(Nuid id)
        {
            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildEntities(id);

            return await db.HashExistsAsync(key, id.Unique.ToString());
        }

        private async Task<string> GetCacheType(Nuid id)
        {
            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildEntities(id);

            return await db.Database.HashGetAsync(key, id.Unique);
        }

        private async Task<bool> SetCacheType(Nuid id, string entity_type)
        {
            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildEntities(id);

            return await db.Database.HashSetAsync(key, id.Unique, entity_type);
        }

        class EntityTransaction
        {
            public Nuid Id
            {
                get
                {
                    return Entity == null ? Nuid.Empty : Entity.Id;
                }
            }

            public string Type
            {
                get
                {
                    return Entity == null ? Global.NULL_STRING : Entity.Type;
                }
            }

            public Task<HashEntry[]> Fields { get; set; }

            public Dictionary<string, Task<HashEntry[]>> Tables { get; private set; }

            public Entity Entity;

            public EntityTransaction(Nuid id, string type)
            {
                Tables = new Dictionary<string, Task<HashEntry[]>>();
                Entity = GenObject(type);
                if (Entity != null)
                {
                    Entity.Id = id;
                }
            }

            public void AddTableTrans(string table_name, Task<HashEntry[]> table)
            {
                if (table == null)
                {
                    return;
                }

                if (!Tables.ContainsKey(table_name))
                {
                    Tables.Add(table_name, table);
                }
                else
                {
                    Tables[table_name] = table;
                }
            }

            private Entity GenObject(string type)
            {
                EntityPrefab entity_prefab = Prefabs.GetEntity(type);
                if (entity_prefab == null)
                {
                    return null;
                }

                Entity gen_entity = new Entity();
                gen_entity.Type = entity_prefab.type;

                foreach (FieldPrefab field_prefab in entity_prefab.fields.Values)
                {
                    switch (field_prefab.type)
                    {
                        case VarType.Bool:
                            gen_entity.CreateField(field_prefab.name, Global.NULL_BOOL);
                            break;
                        case VarType.Int:
                            gen_entity.CreateField(field_prefab.name, Global.NULL_INT);
                            break;
                        case VarType.Float:
                            gen_entity.CreateField(field_prefab.name, Global.NULL_FLOAT);
                            break;
                        case VarType.Long:
                            gen_entity.CreateField(field_prefab.name, Global.NULL_LONG);
                            break;
                        case VarType.String:
                            gen_entity.CreateField(field_prefab.name, Global.NULL_STRING);
                            break;
                        case VarType.Nuid:
                            gen_entity.CreateField(field_prefab.name, Nuid.Empty);
                            break;
                        case VarType.List:
                            gen_entity.CreateField(field_prefab.name, NList.Empty);
                            break;
                        default:
                            break;
                    }
                }

                foreach (TablePrefab table_prefab in entity_prefab.tables.Values)
                {
                    gen_entity.CreateTable(table_prefab.name, table_prefab.primary_key.type);
                }

                return gen_entity;
            }

        } 

        private async Task<IReadOnlyList<Entity>> GetCacheEntities()
        {
            Nuid id = Nuid.New(Identity, Identity);
            IRedisDatabase db = GetCache(id);
            string entites_key = CacheUtils.BuildEntities(id);

            List<Entity> sorts = new List<Entity>();
            List<EntityTransaction> entity_trans = new List<EntityTransaction>();

            try
            {
                HashEntry[] entities = await db.Database.HashGetAllAsync(entites_key);

                foreach (HashEntry member in entities)
                {
                    long child_id = long.Parse(member.Name);
                    string type = member.Value;

                    Nuid entity_id = Nuid.New(child_id, Identity);

                    EntityTransaction trans = new EntityTransaction(entity_id, type);
                    if (trans.Entity == null)
                    {
                        continue;
                    }

                    entity_trans.Add(trans);
                }

                ITransaction query_trans = db.Database.CreateTransaction();
                foreach (EntityTransaction trans in entity_trans)
                {
                    EntityPrefab entity_prefab = Prefabs.GetEntity(trans.Type);
                    if (entity_prefab == null)
                    {
                        throw new Exception($"Prefabs.GetEntity cant found {trans.Type}");
                    }

                    string field_key = CacheUtils.BuildFields(trans.Id);
                    trans.Fields = query_trans.HashGetAllAsync(field_key);

                    foreach (Table table in trans.Entity.GetTables())
                    {
                        TablePrefab table_prefab = entity_prefab.tables[table.GetName()];
                        string table_key = CacheUtils.BuildTable(trans.Id, table.GetName());
                        Task<HashEntry[]> key_values = query_trans.HashGetAllAsync(table_key);
                        trans.AddTableTrans(table.GetName(), key_values);
                    }
                }

                bool redis_execute = await query_trans.ExecuteAsync();
                if (!redis_execute)
                {
                    throw new Exception("query_trans ExecuteAsync ERROR!!");
                }

                foreach (EntityTransaction trans in entity_trans)
                {
                    Entity entity = await BuildCacheEntity(trans);
                    sorts.Add(entity);
                }
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "GetCacheEntities Failed");
            }

            entity_trans.Clear();

            sorts.Sort((x, y) =>
            {
                EntityPrefab entity_x = Prefabs.GetEntity(x.Type);
                EntityPrefab entity_y = Prefabs.GetEntity(y.Type);

                return entity_y.priority - entity_x.priority;
            });

            return sorts;
        }

        async Task<Entity> BuildCacheEntity(EntityTransaction trans)
        {
            EntityPrefab entity_prefab = Prefabs.GetEntity(trans.Type);
            if (entity_prefab == null)
            {
                throw new Exception($"Prefabs.GetEntity cant found {trans.Type}");
            }

            HashEntry[] fields = await trans.Fields;

            foreach (HashEntry entry in fields)
            {
                FieldPrefab field_prefab = entity_prefab.fields[entry.Name];
                if (field_prefab == null)
                {
                    continue;
                }

                switch (field_prefab.type)
                {
                    case VarType.Bool:
                        {
                            SetFieldValue<bool>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    case VarType.Int:
                        {
                            SetFieldValue<int>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    case VarType.Float:
                        {
                            SetFieldValue<float>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    case VarType.Long:
                        {
                            SetFieldValue<long>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    case VarType.String:
                        {
                            SetFieldValue<string>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    case VarType.Nuid:
                        {
                            SetFieldValue<Nuid>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;

                    case VarType.List:
                        {
                            SetFieldValue<NList>(entry.Name, entry.Value, ref trans.Entity);
                        }
                        break;
                    default:
                        break;
                }

                void SetFieldValue<T>(string field_name, RedisValue field_value, ref Entity entity)
                {
                    T value = JsonUtils.ToObject<T>(field_value);

                    Field field = entity.GetField(field_name);
                    if (field == null)
                    {
                        return;
                    }

                    NList res = NList.Empty;
                    field.TrySet(value, out res);
                }
            }

            foreach (KeyValuePair<string, Task<HashEntry[]>> table_task in trans.Tables)
            {
                TablePrefab table_prefab = entity_prefab.tables[table_task.Key];
                if (table_prefab == null)
                {
                    continue;
                }

                Table table_trans = trans.Entity.GetTable(table_task.Key);
                if (table_trans == null)
                {
                    continue;
                }

                HashEntry[] table_key_values = await table_task.Value;

                switch (table_prefab.primary_key.type)
                {
                    case VarType.Bool:
                        {
                            Table<bool> table = table_trans as Table<bool>;
                            foreach (HashEntry entry in table_key_values)
                            {
                                bool key = bool.Parse(entry.Name);
                                NList key_value = JsonUtils.ToObject<NList>(entry.Value);
                                table.TrySetKeyValue(key, key_value, out _);
                            }
                        }
                        break;
                    case VarType.Int:
                        {
                            Table<int> table = table_trans as Table<int>;
                            foreach (HashEntry entry in table_key_values)
                            {
                                int key = int.Parse(entry.Name);
                                NList key_value = JsonUtils.ToObject<NList>(entry.Value);
                                table.TrySetKeyValue(key, key_value, out _);
                            }
                        }
                        break;
                    case VarType.Long:
                        {
                            Table<long> table = table_trans as Table<long>;
                            foreach (HashEntry entry in table_key_values)
                            {
                                long key = long.Parse(entry.Name);
                                NList key_value = JsonUtils.ToObject<NList>(entry.Value);
                                table.TrySetKeyValue(key, key_value, out _);
                            }
                        }
                        break;
                    case VarType.String:
                        {
                            Table<string> table = table_trans as Table<string>;
                            foreach (HashEntry entry in table_key_values)
                            {
                                string key = entry.Name;
                                NList key_value = JsonUtils.ToObject<NList>(entry.Value);
                                table.TrySetKeyValue(key, key_value, out _);
                            }
                        }
                        break;
                    case VarType.Nuid:
                        {
                            Table<Nuid> table = table_trans as Table<Nuid>;
                            foreach (HashEntry entry in table_key_values)
                            {
                                Nuid key = Nuid.Parse(entry.Name);
                                NList key_value = JsonUtils.ToObject<NList>(entry.Value);
                                table.TrySetKeyValue(key, key_value, out _);
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            return trans.Entity;
        }

        private async Task<Entity> GetCacheEntity(Nuid entity_id)
        {
            try
            {
                IRedisDatabase db = GetCache(entity_id);
                ITransaction query_trans = db.Database.CreateTransaction();

                string type = await GetCacheType(entity_id);
                EntityTransaction trans = new EntityTransaction(entity_id, type);

                
                EntityPrefab entity_prefab = Prefabs.GetEntity(trans.Type);
                if (entity_prefab == null)
                {
                    throw new Exception($"Prefabs.GetEntity cant found {trans.Type}");
                }

                string field_key = CacheUtils.BuildFields(trans.Id);
                trans.Fields = query_trans.HashGetAllAsync(field_key);

                foreach (Table table in trans.Entity.GetTables())
                {
                    TablePrefab table_prefab = entity_prefab.tables[table.GetName()];
                    string table_key = CacheUtils.BuildTable(trans.Id, table.GetName());
                    Task<HashEntry[]> key_values = query_trans.HashGetAllAsync(table_key);
                    trans.AddTableTrans(table.GetName(), key_values);
                }

                bool redis_execute = await query_trans.ExecuteAsync();
                if (!redis_execute)
                {
                    throw new Exception("query_trans ExecuteAsync ERROR!!");
                }

                return await BuildCacheEntity(trans);
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "GetCacheEntities Failed");
            }

            return null;
        }

        private async Task SetCacheEntities(IReadOnlyList<Entity> entities)
        {
            IRedisDatabase db = GetCache();
            ITransaction trans = db.Database.CreateTransaction();
            {
                string key = CacheUtils.BuildEntities(Nuid.New(Identity, Identity));
                HashEntry[] hashFields = new HashEntry[entities.Count];
                for (int i = 0; i < entities.Count; i++)
                {
                    hashFields[i] = new HashEntry(entities[i].Id.Unique, entities[i].Type);
                }

                Task _ = trans.HashSetAsync(key, hashFields);
            }

            {
                foreach (Entity entity in entities)
                {
                    EntityPrefab entity_prefab = Prefabs.GetEntity(entity.Type);
                    if (entity_prefab == null)
                    {
                        continue;
                    }

                    string fields_key = CacheUtils.BuildFields(entity.Id);
                    Field[] fields = entity.GetFields();
                    List<HashEntry> cache_fields = new List<HashEntry>();
                    for (int i = 0; i < fields.Length; i++)
                    {
                        FieldPrefab field_prefab = entity_prefab.fields[fields[i].Name];
                        if (field_prefab == null)
                        {
                            continue;
                        }

                        string field_value = "";
                        switch (field_prefab.type)
                        {
                            case VarType.Bool:
                                field_value = JsonUtils.ToJson(fields[i].Get<bool>());
                                break;
                            case VarType.Int:
                                field_value = JsonUtils.ToJson(fields[i].Get<int>());
                                break;
                            case VarType.Long:
                                field_value = JsonUtils.ToJson(fields[i].Get<long>());
                                break;
                            case VarType.Float:
                                field_value = JsonUtils.ToJson(fields[i].Get<float>());
                                break;
                            case VarType.String:
                                field_value = JsonUtils.ToJson(fields[i].Get<string>());
                                break;
                            case VarType.Nuid:
                                field_value = JsonUtils.ToJson(fields[i].Get<Nuid>());
                                break;
                            case VarType.List:
                                field_value = JsonUtils.ToJson(fields[i].Get<NList>());
                                break;
                            default:
                                break;
                        }

                        cache_fields.Add(new HashEntry(fields[i].Name, field_value));
                    }

                    Task _ = trans.HashSetAsync(fields_key, cache_fields.ToArray());

                    Table[] tables = entity.GetTables();
                    foreach (Table table in tables)
                    {
                        string table_key = CacheUtils.BuildTable(entity.Id, table.GetName());

                        TablePrefab table_prefab = entity_prefab.tables[table.GetName()];
                        if (table_prefab == null)
                        {
                            continue;
                        }

                        List<HashEntry> cache_key_values = new List<HashEntry>();

                        void SetCacheKeyValue<TPrimaryKey>()
                        {
                            Table<TPrimaryKey> t = table as Table<TPrimaryKey>;
                            IReadOnlyList<TPrimaryKey> keys = t.GetPrimaryKeys();
                            foreach (TPrimaryKey key in keys)
                            {
                                string json = JsonUtils.ToJson(t.GetKeyValue(key));
                                cache_key_values.Add(new HashEntry(key.ToString(), json));
                            }
                        }
                        switch (table_prefab.primary_key.type)
                        {
                            case VarType.Bool:
                                SetCacheKeyValue<bool>();
                                break;
                            case VarType.Int:
                                SetCacheKeyValue<int>();
                                break;
                            case VarType.Long:
                                SetCacheKeyValue<long>();
                                break;
                            case VarType.Float:
                                SetCacheKeyValue<float>();
                                break;
                            case VarType.String:
                                SetCacheKeyValue<string>();
                                break;
                            case VarType.Nuid:
                                SetCacheKeyValue<Nuid>();
                                break;
                            default:
                                break;
                        }

                        Task __ = trans.HashSetAsync(table_key, cache_key_values.ToArray());
                    }
                }

                bool redis_execute = await trans.ExecuteAsync();
                if (!redis_execute)
                {
                    throw new Exception("trans ExecuteAsync ERROR!!");
                }
            }
        }

        #region ------ ------ ------ ------ ------ ------ ------Field------ ------ ------ ------ ------ ------ ------
        private async Task<bool> SetCacheField<T>(Nuid id, string field_name, T field_value)
        {
            if (!await CacheExist(id))
            {
                _Logger.LogError("'{0} SetCacheField error for not exist", id);
                return false;
            }

            bool result = false;
            try
            {
                IRedisDatabase db = GetCache(id);
                string key = CacheUtils.BuildFields(id);
                result = await db.HashSetAsync(key, field_name, field_value);
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "'{0} SetCacheField error for field={1} value={2}", id, field_name, field_value);
            }

            return result;
        }

        private async Task<T> GetCacheField<T>(Nuid id, string field_name)
        {
            if (!await CacheExist(id))
            {
                return default(T);
            }

            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildFields(id);
            return await db.HashGetAsync<T>(key, field_name);
        }
        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Table------ ------ ------ ------ ------ ------ ------

        private async Task<NList> GetCacheKeys(Nuid id, string table_name)
        {
            if (!await CacheExist(id))
            {
                return NList.Empty;
            }

            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildTable(id, table_name);

            string type = await GetCacheType(id);
            EntityPrefab entity_prefab = Prefabs.GetEntity(type);
            if (entity_prefab == null)
            {
                return NList.Empty;
            }

            TablePrefab table_prefab = entity_prefab.tables[table_name];
            if (table_prefab == null)
            {
                return NList.Empty;
            }

            NList rows = NList.New();
            foreach (string hash_key in await db.HashKeysAsync(key))
            {
                switch (table_prefab.primary_key.type)
                {
                    case VarType.Bool:
                        {
                            bool row = bool.Parse(hash_key);
                            rows.Add(row);
                        }
                        break;
                    case VarType.Int:
                        {
                            int row = int.Parse(hash_key);
                            rows.Add(row);
                        }
                        break;
                    case VarType.Long:
                        {
                            long row = long.Parse(hash_key);
                            rows.Add(row);
                        }
                        break;
                    case VarType.Nuid:
                        {
                            Nuid row = Nuid.Parse(hash_key);
                            rows.Add(row);
                        }
                        break;
                    case VarType.String:
                        {
                            string row = hash_key;
                            rows.Add(row);
                        }
                        break;
                    default:
                        break;
                }
            }

            return rows.Count > 0 ? rows : NList.Empty;
        }

        private async Task<NList> GetCacheTableKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key)
        {
            if (!await CacheExist(id))
            {
                return NList.Empty;
            }

            IRedisDatabase db = GetCache(id);
            string key = CacheUtils.BuildTable(id, table_name);
            return await db.HashGetAsync<NList>(key, primary_key.ToString());
        }

        private async Task<bool> SetCacheTableKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, NList value)
        {
            if (!await CacheExist(id))
            {
                return false;
            }

            bool result = false;
            try
            {
                IRedisDatabase db = GetCache(id);
                string key = CacheUtils.BuildTable(id, table_name);
                result = await db.HashSetAsync(key, primary_key.ToString(), value);
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "'{0} SetCacheRow error for table={1} row={2} value={3}", id, table_name, primary_key, value);
            }

            return result;
        }

        private async Task<bool> DelCacheTableKey<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key)
        {
            if (!await CacheExist(id))
            {
                return false;
            }

            bool result = false;
            try
            {
                IRedisDatabase db = GetCache(id);
                string key = CacheUtils.BuildTable(id, table_name);
                result = await db.HashDeleteAsync(key, primary_key.ToString());
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "'{0} DelCacheRow error for table={1} row={2}", id, table_name, primary_key);
            }

            return result;
        }

        private async Task<bool> ClearCacheTable(Nuid id, string table_name)
        {
            if (!await CacheExist(id))
            {
                return false;
            }

            bool result = false;
            try
            {
                IRedisDatabase db = GetCache(id);
                string key = CacheUtils.BuildTable(id, table_name);
                result = await db.RemoveAsync(key);
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "'{0} ClearCacheTable error for table={1}", id, table_name);
            }

            return result;
        }
        #endregion
    }
}
