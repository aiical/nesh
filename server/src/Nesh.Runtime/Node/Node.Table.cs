using Microsoft.Extensions.Logging;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    public partial class Node
    {
        #region ------ ------ ------ ------ ------ ------ ------SetCol------ ------ ------ ------ ------ ------ ------
        private async Task SetKeyCol<TColValue, TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, TColValue col_value)
        {
            if (col_value == null)
            {
                _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when col_value is null!");
                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when Grain dont found entity!");
                    return;
                }

                Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                if (table == null)
                {
                    _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when Grain dont found field!");
                    return;
                }

                NList result;
                if (!table.TrySetKeyCol(primary_key, col, col_value, out result))
                {
                    _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when {col_value} TrySetRowCol failed!");
                    return;
                }

                await CallbackTable(id, table_name, TableEvent.SetCol, result);
            }
            else if (NodeType == NodeType.Cache)
            {
                NList value = await GetCacheTableKeyValue(id, table_name, primary_key);
                TColValue old_value = value.Get<TColValue>(col);
                if (old_value.Equals(col_value))
                {
                    _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when new=old SetCacheRow failed!");
                }

                value.Set(col, col_value);

                if (!await SetCacheTableKeyValue(id, table_name, primary_key, value))
                {
                    _Logger.LogError($"{id} SetKeyCol {table_name} {primary_key}_{col} when {value} SetCacheTableKeyValue failed!");
                    return;
                }

                NList result = NList.New();
                result.Add(primary_key);
                result.Add(col);
                result.Add(old_value);
                result.Add(col_value);

                await CallbackTable(id, table_name, TableEvent.SetCol, result);
            }
        }

        public async Task SetColBool<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, bool col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColBool(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColInt<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, int col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColInt(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColLong<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, long col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColLong(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColFloat<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, float col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColFloat(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColString<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, string col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColString(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColId<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, Nuid col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColId(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        public async Task SetColList<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, NList col_value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetColList(id, table_name, primary_key, col, col_value);
                return;
            }

            await SetKeyCol(id, table_name, primary_key, col, col_value);
        }

        #endregion

        #region ------ ------ ------ ------ ------ ------ ------GetCol------ ------ ------ ------ ------ ------ ------
        private async Task<TColValue> GetKeyCol<TColValue, TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);

                if (entity != null)
                {
                    Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                    if (table == null)
                    {
                        return default(TColValue);
                    }

                    return table.GetCol<TColValue>(primary_key, col);
                }
            }
            else if (NodeType == NodeType.Cache)
            {
                NList row_value = await GetCacheTableKeyValue(id, table_name, primary_key);
                return row_value.Get<TColValue>(col);
            }

            return default(TColValue);
        }

        public async Task<bool> GetColBool<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColBool(id, table_name, primary_key, col);
            }

            return await GetKeyCol<bool, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<int> GetColInt<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColInt(id, table_name, primary_key, col);
            }

            return await GetKeyCol<int, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<long> GetColLong<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColLong(id, table_name, primary_key, col);
            }

            return await GetKeyCol<long, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<float> GetColFloat<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColFloat(id, table_name, primary_key, col);
            }

            return await GetKeyCol<float, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<string> GetColString<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColString(id, table_name, primary_key, col);
            }

            return await GetKeyCol<string, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<Nuid> GetColId<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColId(id, table_name, primary_key, col);
            }

            return await GetKeyCol<Nuid, TPrimaryKey>(id, table_name, primary_key, col);
        }

        public async Task<INList> GetColList<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetColList(id, table_name, primary_key, col);
            }

            return await GetKeyCol<NList, TPrimaryKey>(id, table_name, primary_key, col);
        }
        #endregion

        public async Task AddKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, NList value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.AddKeyValue(id, table_name, primary_key, value);

                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} AddKeyValue {table_name} when Grain dont found entity!");
                    return;
                }

                Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                if (table == null)
                {
                    _Logger.LogError($"{id} AddKeyValue {table_name} when Grain dont found table!");
                    return;
                }

                NList result;
                if (!table.TrySetKeyValue(primary_key, value, out result))
                {
                    _Logger.LogError($"{id} AddKeyValue {table_name} when Grain found row exist {primary_key}!");
                    return;
                }

                await CallbackTable(id, table_name, TableEvent.AddKey, result);
            }
            else if (NodeType == NodeType.Cache)
            {
                if (!await SetCacheTableKeyValue(id, table_name, primary_key, value))
                {
                    _Logger.LogError($"{id} AddKeyValue {table_name} when SetCacheTableKeyValue failed {primary_key} {value}!");
                    return;
                }

                NList result = NList.New();
                result.Add(primary_key);
                result.Add(value);

                await CallbackTable(id, table_name, TableEvent.AddKey, result);
            }
        }

        public async Task SetKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, NList value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetKeyValue(id, table_name, primary_key, value);

                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} SetKeyValue {table_name} when Grain dont found entity!");
                    return;
                }

                Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                if (table == null)
                {
                    _Logger.LogError($"{id} SetKeyValue {table_name} when Grain dont found table!");
                    return;
                }

                NList result;
                if (!table.TrySetKeyValue(primary_key, value, out result))
                {
                    _Logger.LogError($"{id} SetKeyValue {table_name} when Grain dont found primary_key {primary_key}!");
                    return;
                }

                await CallbackTable(id, table_name, TableEvent.SetKey, result);
            }
            else if (NodeType == NodeType.Cache)
            {
                if (await SetCacheTableKeyValue(id, table_name, primary_key, value))
                {
                    _Logger.LogError($"{id} SetCacheTableKeyValue {table_name} when Grain dont found primary_key {primary_key}!");
                    return;
                }

                NList result = NList.New();
                result.Add(primary_key);
                result.Add(value);
                await CallbackTable(id, table_name, TableEvent.SetKey, result);
            }
        }

        public async Task ClearTable(Nuid id, string table_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.ClearTable(id, table_name);

                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} ClearTable {table_name} when Grain dont found entity!");
                    return;
                }

                Table table = entity.GetTable(table_name);
                if (table == null)
                {
                    _Logger.LogError($"{id} ClearTable {table_name} when Grain dont found table!");
                    return;
                }

                table.Clear();

                await CallbackTable(id, table_name, TableEvent.Clear, NList.Empty);
            }
            else if (NodeType == NodeType.Cache)
            {
                if (!await ClearCacheTable(id, table_name))
                {
                    _Logger.LogError($"{id} ClearCacheTable {table_name} failed!");
                    return;
                }

                await CallbackTable(id, table_name, TableEvent.Clear, NList.Empty);
            }
        }

        public async Task DelKey<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.DelKey(id, table_name, primary_key);

                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} DelKey {table_name} when Grain dont found entity!");
                    return;
                }

                Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                if (table == null)
                {
                    _Logger.LogError($"{id} DelKey {table_name} when Grain dont found table!");
                    return;
                }

                NList result;
                if (!table.TryDelKey(primary_key, out result))
                {
                    _Logger.LogError($"{id} DelKey {table_name} {primary_key} when Grain dont found primary_key!");
                    return;
                }

                await CallbackTable(id, table_name, TableEvent.DelKey, result);
            }
            else if (NodeType == NodeType.Cache)
            {
                NList row_value = await GetCacheTableKeyValue(id, table_name, primary_key);
                if (!await DelCacheTableKey(id, table_name, primary_key))
                {
                    _Logger.LogError($"{id} DelCacheTableKey {table_name} {primary_key} when Grain dont found row!");
                    return;
                }

                NList result = NList.New();
                result.Add(primary_key);
                result.Add(row_value);
                await CallbackTable(id, table_name, TableEvent.DelKey, result);
            }
        }

        public async Task<INList> GetKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetKeyValue(id, table_name, primary_key);
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} GetKeyValue {table_name} when Grain dont found entity!");
                    return NList.Empty;
                }

                Table<TPrimaryKey> table = entity.GetTable(table_name) as Table<TPrimaryKey>;
                if (table == null)
                {
                    _Logger.LogError($"{id} GetKeyValue {table_name} when Grain dont found table!");
                    return NList.Empty;
                }

                return table.GetKeyValue(primary_key);
            }
            else if (NodeType == NodeType.Cache)
            {
                return await GetCacheTableKeyValue(id, table_name, primary_key);
            }

            return NList.Empty;
        }

        public async Task<INList> GetKeys(Nuid id, string table_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetKeys(id, table_name);
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} GetKeys {table_name} when Grain dont found entity!");
                    return NList.Empty;
                }

                Table table = entity.GetTable(table_name);
                if (table == null)
                {
                    _Logger.LogError($"{id} GetKeys {table_name} when Grain dont found table!");
                    return NList.Empty;
                }

                return table.GetKeys();
            }
            else if (NodeType == NodeType.Cache)
            {
                return await GetCacheKeys(id, table_name);
            }

            return NList.Empty;
        }
    }
}
