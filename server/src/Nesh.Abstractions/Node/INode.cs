using Nesh.Abstractions.Agent;
using Nesh.Abstractions.Data;
using Orleans;
using System.Threading.Tasks;

namespace Nesh.Abstractions
{
    public enum NodeType
    {
        None  = 0,
        Grain = 1,
        Cache = 2,
    }

    public interface INode : IGrainWithIntegerKey
    {
        Task BindAgent(IAgent agent);

        Task<INode> CreateNode<AGENT>() where AGENT : IAgent;

        Task<bool> IsActive();
        Task Active();
        Task Deactive();

        Task Command(Nuid id, int command, NList command_msg);
        Task Custom(Nuid id, int custom, NList custom_msg);

        #region ------ ------ ------ ------ ------ ------ ------Entity------ ------ ------ ------ ------ ------ ------
        Task<bool> Exists(Nuid id);
        Task<string> GetType(Nuid id);
        Task<Nuid> Create(Nuid id, string type, NList args);
        Task<Nuid> Create(string type, Nuid origin, NList args);
        Task Entry(Nuid id);
        Task Leave(Nuid id);
        Task Destroy(Nuid id);
        Task<NList> GetEntities();
        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Field------ ------ ------ ------ ------ ------ ------
        Task SetFieldBool(Nuid id, string field_name, bool value);
        Task SetFieldInt(Nuid id, string field_name, int value);
        Task SetFieldLong(Nuid id, string field_name, long value);
        Task SetFieldFloat(Nuid id, string field_name, float value);
        Task SetFieldString(Nuid id, string field_name, string value);
        Task SetFieldId(Nuid id, string field_name, Nuid value);
        Task SetFieldList(Nuid id, string field_name, NList value);

        Task<bool> GetFieldBool(Nuid id, string field_name);
        Task<int> GetFieldInt(Nuid id, string field_name);
        Task<long> GetFieldLong(Nuid id, string field_name);
        Task<float> GetFieldFloat(Nuid id, string field_name);
        Task<string> GetFieldString(Nuid id, string field_name);
        Task<Nuid> GetFieldId(Nuid id, string field_name);
        Task<INList> GetFieldList(Nuid id, string field_name);
        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Table------ ------ ------ ------ ------ ------ ------
        Task<bool> GetColBool<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<int> GetColInt<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<long> GetColLong<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<float> GetColFloat<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<string> GetColString<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<Nuid> GetColId<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);
        Task<INList> GetColList<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col);

        Task SetColBool<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, bool col_value);
        Task SetColInt<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, int col_value);
        Task SetColLong<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, long col_value);
        Task SetColFloat<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, float col_value);
        Task SetColString<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, string col_value);
        Task SetColId<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, Nuid col_value);
        Task SetColList<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, int col, NList col_value);

        Task<INList> GetKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key);
        Task<INList> GetKeys(Nuid id, string table_name);
        Task AddKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, NList value);
        Task SetKeyValue<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key, NList value);
        Task DelKey<TPrimaryKey>(Nuid id, string table_name, TPrimaryKey primary_key);
        Task ClearTable(Nuid id, string table_name);
        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Timer------ ------ ------ ------ ------ ------ ------
        Task AddCountdown(Nuid id, string timer, long over_millseconds, NEventCallback handler);
        Task AddHeartbeat(Nuid id, string timer, long gap_millseconds, int count, NEventCallback handler);
        Task<bool> HasTimer(Nuid id, string timer);
        Task DelTimer(Nuid id, string timer);
        #endregion

        #region ------ ------ ------ ------ ------ ------ ------Logger------ ------ ------ ------ ------ ------ ------
        Task Info(string message);
        Task Warn(string message);
        Task Error(string message);
        #endregion
    }
}
