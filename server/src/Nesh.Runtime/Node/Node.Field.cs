using Microsoft.Extensions.Logging;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    public partial class Node
    {
        #region ------ ------ ------ ------ ------ ------ ------Set------ ------ ------ ------ ------ ------ ------
        private async Task SetField<T>(Nuid id, string field_name, T field_value)
        {
            if (field_value == null)
            {
                _Logger.LogError($"{id} SetField {field_name} when field_value is null!");
                return;
            }

            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    _Logger.LogError($"{id} SetField {field_name} when Grain dont found entity!");
                    return;
                }

                Field field = entity.GetField(field_name);
                if (field == null)
                {
                    _Logger.LogError($"{id} SetField {field_name} when Grain dont found field!");
                    return;
                }

                NList result;
                if (!field.TrySet(field_value, out result))
                {
                    _Logger.LogError($"{id} SetField {field_name} when {field_value} TrySet failed!");
                    return;
                }

                await CallbackField(id, field_name, FieldEvent.Change, result);
            }
            else if (NodeType == NodeType.Cache)
            {
                T old_value = await GetCacheField<T>(id, field_name);
                if (!await SetCacheField(id, field_name, field_value))
                {
                    return;
                }

                NList result = NList.New();
                result.Add(old_value);
                result.Add(field_value);
                await CallbackField(id, field_name, FieldEvent.Change, result);
            }
        }

        public async Task SetFieldBool(Nuid id, string field_name, bool value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldBool(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldInt(Nuid id, string field_name, int value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldInt(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldLong(Nuid id, string field_name, long value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldLong(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldFloat(Nuid id, string field_name, float value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldFloat(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldString(Nuid id, string field_name, string value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldString(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldId(Nuid id, string field_name, Nuid value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldId(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        public async Task SetFieldList(Nuid id, string field_name, NList value)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                await node.SetFieldList(id, field_name, value);
                return;
            }

            await SetField(id, field_name, value);
        }

        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Get------ ------ ------ ------ ------ ------ ------

        private async Task<T> GetField<T>(Nuid id, string field_name)
        {
            if (NodeType == NodeType.Grain)
            {
                Entity entity = EntityManager.Get(id);
                if (entity == null)
                {
                    return default(T);
                }

                Field field = entity.GetField(field_name);
                if (field == null)
                {
                    return default(T);
                }

                return field.Get<T>();
            }
            else if (NodeType == NodeType.Cache)
            {
                return await GetCacheField<T>(id, field_name);
            }

            return default(T);
        }

        public async Task<bool> GetFieldBool(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldBool(id, field_name);
            }

            return await GetField<bool>(id, field_name);
        }

        public async Task<int> GetFieldInt(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldInt(id, field_name);
            }

            return await GetField<int>(id, field_name);
        }

        public async Task<long> GetFieldLong(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldLong(id, field_name);
            }

            return await GetField<long>(id, field_name);
        }

        public async Task<float> GetFieldFloat(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldFloat(id, field_name);
            }

            return await GetField<float>(id, field_name);
        }

        public async Task<string> GetFieldString(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldString(id, field_name);
            }

            return await GetField<string>(id, field_name);
        }

        public async Task<Nuid> GetFieldId(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldId(id, field_name);
            }

            return await GetField<Nuid>(id, field_name);
        }

        public async Task<INList> GetFieldList(Nuid id, string field_name)
        {
            if (id.Origin != Identity)
            {
                INode node = GrainFactory.GetGrain<INode>(id.Origin);
                return await node.GetFieldList(id, field_name);
            }

            return await GetField<INList>(id, field_name);
        }

        #endregion
    }
}
