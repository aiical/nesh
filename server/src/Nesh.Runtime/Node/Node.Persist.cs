using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Storage.Models;
using Nesh.Abstractions.Utils;
using Nesh.Runtime.Utils;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    public partial class Node
    {
        private async Task SavePersistEntities()
        {
            IReadOnlyList<Entity> entities = null;
            if (NodeType == Abstractions.NodeType.Grain)
            {
                entities = EntityManager.GetEntities();
            }
            else if (NodeType == Abstractions.NodeType.Cache)
            {
                entities = await GetCacheEntities();
            }

            if (entities == null || entities.Count == 0)
            {
                return;
            }

            EntityList entity_list = new EntityList();
            entity_list.entities = new List<EntityChild>();
            entity_list.origin = Identity;
            entity_list.node = NodeType;

            var database = _IMongoClient.GetDatabase(PersistUtils.ENTITY_DB);

            foreach (Entity entity in entities)
            {
                EntityPrefab entity_prefab = Prefabs.GetEntity(entity.Type);
                if (entity_prefab == null)
                {
                    continue;
                }

                Dictionary<string, object> entity_models = new Dictionary<string, object>();

                entity_models.Add(Global.MARK_UNIQUE, entity.Id.Unique);

                Field[] fields = entity.GetFields();
                foreach (Field field in fields)
                {
                    FieldPrefab field_prefab = entity_prefab.fields[field.Name];
                    if (field_prefab == null)
                    {
                        continue;
                    }
                    if (!field_prefab.save)
                    {
                        continue;
                    }

                    switch (field_prefab.type)
                    {
                        case VarType.Bool:
                            {
                                entity_models.Add(field_prefab.name, field.Get<bool>());
                            }
                            break;
                        case VarType.Int:
                            {
                                entity_models.Add(field_prefab.name, field.Get<int>());
                            }
                            break;
                        case VarType.Float:
                            {
                                entity_models.Add(field_prefab.name, field.Get<float>());
                            }
                            break;
                        case VarType.Long:
                            {
                                entity_models.Add(field_prefab.name, field.Get<long>());
                            }
                            break;
                        case VarType.Nuid:
                            {
                                BsonDocument document = BsonDocument.Parse(JsonUtils.ToJson(field.Get<Nuid>()));
                                entity_models.Add(field_prefab.name, document);
                            }
                            break;
                        case VarType.String:
                            {
                                entity_models.Add(field_prefab.name, field.Get<string>());
                            }
                            break;
                        case VarType.List:
                            {
                                BsonDocument document = BsonDocument.Parse(JsonUtils.ToJson(field.Get<NList>()));
                                entity_models.Add(field_prefab.name, document);
                            }
                            break;
                        default:
                            break;
                    }
                }

                Table[] tables = entity.GetTables();
                foreach (Table base_table in tables)
                {
                    BsonArray table_model = new BsonArray();
                    TablePrefab table_prefab = entity_prefab.tables[base_table.GetName()];

                    switch (table_prefab.primary_key.type)
                    {
                        case VarType.Bool:
                            {
                                UpdateTableKeyValue<bool>();
                            }
                            break;
                        case VarType.Int:
                            {
                                UpdateTableKeyValue<int>();
                            }
                            break;
                        case VarType.Long:
                            {
                                UpdateTableKeyValue<long>();
                            }
                            break;
                        case VarType.Float:
                            {
                                UpdateTableKeyValue<float>();
                            }
                            break;
                        case VarType.String:
                            {
                                UpdateTableKeyValue<string>();
                            }
                            break;
                        case VarType.Nuid:
                            {
                                UpdateTableKeyValue<Nuid>();
                            }
                            break;
                        default:
                            break;
                    }

                    void UpdateTableKeyValue<TPrimaryKey>()
                    {
                        Table<TPrimaryKey> table = base_table as Table<TPrimaryKey>;

                        foreach (TPrimaryKey key in table.GetPrimaryKeys())
                        {
                            Dictionary<string, object> key_value_models = new Dictionary<string, object>();
                            key_value_models.Add(nameof(TablePrefab.primary_key), key);
                            NList key_value = table.GetKeyValue(key);

                            for (int col = 0; col < table_prefab.cols; col++)
                            {
                                TablePrefab.ColumnPrefab col_prefab = table_prefab.columns[col];
                                switch (col_prefab.type)
                                {
                                    case VarType.Bool:
                                        {
                                            bool col_value = key_value.Get<bool>(col);
                                            key_value_models.Add(col_prefab.name, col_value);
                                        }
                                        break;
                                    case VarType.Int:
                                        {
                                            int col_value = key_value.Get<int>(col);
                                            key_value_models.Add(col_prefab.name, col_value);
                                        }
                                        break;
                                    case VarType.Long:
                                        {
                                            long col_value = key_value.Get<long>(col);
                                            key_value_models.Add(col_prefab.name, col_value);
                                        }
                                        break;
                                    case VarType.Float:
                                        {
                                            float col_value = key_value.Get<float>(col);
                                            key_value_models.Add(col_prefab.name, col_value);
                                        }
                                        break;
                                    case VarType.String:
                                        {
                                            string col_value = key_value.Get<string>(col);
                                            key_value_models.Add(col_prefab.name, col_value);
                                        }
                                        break;
                                    case VarType.Nuid:
                                        {
                                            Nuid col_value = key_value.Get<Nuid>(col);
                                            key_value_models.Add(col_prefab.name, BsonDocument.Parse(JsonUtils.ToJson(col_value)));
                                        }
                                        break;
                                    case VarType.List:
                                        {
                                            NList col_value = key_value.Get<NList>(col);
                                            key_value_models.Add(col_prefab.name, BsonDocument.Parse(JsonUtils.ToJson(col_value)));
                                        }
                                        break;
                                    default:
                                        break;
                                }
                            }

                            table_model.Add(new BsonDocument(key_value_models));
                        }
                    }

                    entity_models.Add(base_table.GetName(), table_model);
                }

                entity_list.entities.Add(new EntityChild() { unique = entity.Id.Unique, type = entity.Type, entity = new BsonDocument(entity_models) });
            }

            var collection = database.GetCollection<EntityList>(PersistUtils.ENTITIES);

            var filter = Builders<EntityList>.Filter.And(Builders<EntityList>.Filter.Eq(n => n.origin, Identity));
            EntityList found = await collection.FindOneAndReplaceAsync(filter, entity_list);
            if (found == null)
            {
                await collection.InsertOneAsync(entity_list);
            }
        }

        private async Task<IReadOnlyList<Entity>> LoadPersistEntities()
        {
            var database = _IMongoClient.GetDatabase(PersistUtils.ENTITY_DB);

            try
            {
                var collection = database.GetCollection<EntityList>(PersistUtils.ENTITIES);

                var filter = Builders<EntityList>.Filter.Eq(n => n.origin, Identity);
                IAsyncCursor<EntityList> res = await collection.FindAsync(filter);

                EntityList entity_list = await res.FirstOrDefaultAsync();
                if (entity_list == null)
                {
                    return null;
                }

                List<Entity> entities = new List<Entity>();

                foreach (EntityChild entity_child in entity_list.entities)
                {
                    EntityPrefab entity_prefab = Prefabs.GetEntity(entity_child.type);
                    if (entity_prefab == null)
                    {
                        continue;
                    }

                    BsonDocument doc = entity_child.entity;
                    long unique = doc.GetValue(Global.MARK_UNIQUE).AsInt64;
                    Entity entity = Entity.Gen(entity_child.type);
                    entity.Id = Nuid.New(unique, Identity);

                    foreach (FieldPrefab field_prefab in entity_prefab.fields.Values)
                    {
                        if (!field_prefab.save)
                        {
                            continue;
                        }

                        Field field = entity.GetField(field_prefab.name);
                        if (field == null)
                        {
                            continue;
                        }

                        BsonValue bsonValue = doc.GetValue(field_prefab.name);
                        switch (field_prefab.type)
                        {
                            case VarType.Bool:
                                {
                                    bool value = bsonValue.AsBoolean;
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.Int:
                                {
                                    int value = bsonValue.AsInt32;
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.Long:
                                {
                                    long value = bsonValue.AsInt64;
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.Float:
                                {
                                    float value = (float)bsonValue.AsDouble;
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.String:
                                {
                                    string value = bsonValue.AsString;
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.Nuid:
                                {
                                    string value = bsonValue.AsBsonDocument.ToJson();
                                    Nuid nuid = JsonUtils.ToObject<Nuid>(value);
                                    field.TrySet(value, out _);
                                }
                                break;
                            case VarType.List:
                                {
                                    string value = bsonValue.AsBsonDocument.ToJson();
                                    NList lst = JsonUtils.ToObject<NList>(value);
                                    field.TrySet(value, out _);
                                }
                                break;
                            default:
                                break;
                        }
                    }

                    foreach (TablePrefab table_prefab in entity_prefab.tables.Values)
                    {
                        if (!table_prefab.save)
                        {
                            continue;
                        }

                        BsonArray bsonarr = doc.GetValue(table_prefab.name) as BsonArray;
                        foreach (BsonDocument key_value_bson in bsonarr.Values)
                        {
                            BsonValue pk_bson = key_value_bson.GetValue(nameof(TablePrefab.primary_key));
                            switch (table_prefab.primary_key.type)
                            {
                                case VarType.Bool:
                                    {
                                        bool value = pk_bson.AsBoolean;
                                        LoadTableKeyValue(value);
                                    }
                                    break;
                                case VarType.Int:
                                    {
                                        int value = pk_bson.AsInt32;
                                        LoadTableKeyValue(value);
                                    }
                                    break;
                                case VarType.Long:
                                    {
                                        long value = pk_bson.AsInt64;
                                        LoadTableKeyValue(value);
                                    }
                                    break;
                                case VarType.String:
                                    {
                                        string value = pk_bson.AsString;
                                        LoadTableKeyValue(value);
                                    }
                                    break;
                                case VarType.Nuid:
                                    {
                                        string value = pk_bson.AsBsonDocument.ToJson();
                                        Nuid nuid = JsonUtils.ToObject<Nuid>(value);
                                        LoadTableKeyValue(nuid);
                                    }
                                    break;
                                default:
                                    break;
                            }

                            void LoadTableKeyValue<TPrimaryKey>(TPrimaryKey primary_key)
                            {
                                Table<TPrimaryKey> table = entity.GetTable(table_prefab.name) as Table<TPrimaryKey>;

                                NList key_value = NList.New();

                                for (int col = 0; col < table_prefab.cols; col++)
                                {
                                    TablePrefab.ColumnPrefab column = table_prefab.columns[col];

                                    BsonValue col_bson = key_value_bson.GetValue(column.name);
                                    switch (column.type)
                                    {
                                        case VarType.Bool:
                                            {
                                                bool value = col_bson.AsBoolean;
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.Int:
                                            {
                                                int value = col_bson.AsInt32;
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.Long:
                                            {
                                                long value = col_bson.AsInt64;
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.Float:
                                            {
                                                float value = (float)col_bson.AsDouble;
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.String:
                                            {
                                                string value = col_bson.AsString;
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.Nuid:
                                            {
                                                string value = col_bson.AsBsonDocument.ToJson();
                                                Nuid nuid = JsonUtils.ToObject<Nuid>(value);
                                                key_value.Add(value);
                                            }
                                            break;
                                        case VarType.List:
                                            {
                                                string value = col_bson.AsBsonDocument.ToJson();
                                                NList lst = JsonUtils.ToObject<NList>(value);
                                                key_value.Add(value);
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                }

                                table.TrySetKeyValue(primary_key, key_value, out _);
                            }
                        }
                    }

                    entities.Add(entity);
                }

                return entities;
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "LoadPersisitEntities Failed!");
            }

            return null;
        }
    }
}
