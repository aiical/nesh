using Newtonsoft.Json;
using Orleans.Concurrency;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Nesh.Abstractions.Data
{
    [Immutable, Serializable]
    [ProtoContract, JsonObject]
    [ProtoInclude(Global.PROTO_TYPE_BOOL, typeof(Table<bool>))]
    [ProtoInclude(Global.PROTO_TYPE_INT, typeof(Table<int>))]
    [ProtoInclude(Global.PROTO_TYPE_FLOAT, typeof(Table<float>))]
    [ProtoInclude(Global.PROTO_TYPE_LONG, typeof(Table<long>))]
    [ProtoInclude(Global.PROTO_TYPE_STRING, typeof(Table<string>))]
    [ProtoInclude(Global.PROTO_TYPE_NUID, typeof(Table<Nuid>))]
    public abstract class Table
    {
        public abstract void Clear();

        public abstract string GetName();

        public abstract INList GetKeys();
    }

    [Immutable, Serializable]
    [ProtoContract, JsonObject]
    public sealed class Table<TPrimaryKey> : Table
    {
        [ProtoMember(Global.PROTO_TABLE_NAME), JsonProperty]
        public string Name { get; private set; }

        [ProtoMember(Global.PROTO_TABLE_KEY_VALUES), JsonProperty]
        private Dictionary<TPrimaryKey, NList> _KeyValues;

        public Table()
        {
            _KeyValues = new Dictionary<TPrimaryKey, NList>();
        }

        public Table(string name) : this()
        {
            Name = name;
        }

        [JsonIgnore]
        public bool IsEmpty { get { return _KeyValues.Count == 0; } }

        public override void Clear()
        {
            _KeyValues.Clear();
        }

        public override string GetName()
        {
            return Name;
        }

        public bool TrySetKeyValue(TPrimaryKey primary_key, NList value, out NList result)
        {
            result = null;

            if (value == null) return false;

            result = NList.New();
            result.Add(primary_key);
            result.Add(value);

            if (!_KeyValues.ContainsKey(primary_key))
            {
                _KeyValues.Add(primary_key, value);
            }
            else
            {
                _KeyValues[primary_key] = value;
            }

            return true;
        }

        public bool TryDelKey(TPrimaryKey primary_key, out NList result)
        {
            result = null;

            NList value = NList.Empty;
            if (!_KeyValues.TryGetValue(primary_key, out value))
            {
                return false;
            }

            _KeyValues.Remove(primary_key);

            result = NList.New();
            result.Add(primary_key).Add(value);
            return true;
        }

        public bool TrySetKeyCol<T>(TPrimaryKey primary_key, int col, T col_value, out NList result)
        {
            result = null;
            if (col_value == null) return false;

            NList row_value;
            if (!_KeyValues.TryGetValue(primary_key, out row_value))
            {
                return false;
            }

            T var = row_value.Get<T>(col);
            if (var == null)
            {
                return false;
            }

            T new_value = col_value;

            if (var.Equals(new_value))
            {
                return false;
            }

            row_value.Set(col, new_value);

            result = NList.New();
            result.Add(primary_key);
            result.Add(col);
            result.Add(var);
            result.Add(new_value);

            return true;
        }

        public T GetCol<T>(TPrimaryKey primary_key, int col)
        {
            NList value;
            if (!_KeyValues.TryGetValue(primary_key, out value))
            {
                return default(T);
            }

            return value.Get<T>(col);
        }

        public NList GetKeyValue(TPrimaryKey primary_key)
        {
            NList value;
            if (!_KeyValues.TryGetValue(primary_key, out value))
            {
                return null;
            }

            return value;
        }

        public override INList GetKeys()
        {
            NList list = NList.New();
            foreach (TPrimaryKey row in _KeyValues.Keys)
            {
                list.Add(row);
            }

            return list;
        }

        public IReadOnlyList<TPrimaryKey> GetPrimaryKeys()
        {
            return _KeyValues.Keys.ToList();
        }
    }
}
