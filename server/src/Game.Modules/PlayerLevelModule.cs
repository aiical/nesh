using Game.Resources.Entity;
using Game.Resources.Msg;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using System.Threading.Tasks;

namespace Game.Module
{
    class PlayerLevelModule : NModule
    {
        protected override void OnInit()
        {
            RCField(Player.TYPE, Player.Fields.LEVEL, FieldEvent.Change, OnLevelChange);
            RCEntity(Player.TYPE, EntityEvent.OnCreate, OnPlayerCreate);

            RCCommand(CommandMsg.TEST_CMD, OnCommand_Test);
        }

        private static async Task OnCommand_Test(INode node, Nuid id, INList args)
        {
            Nuid target_id = args.Get<Nuid>(0);

            int level = await node.GetFieldInt(id, Player.Fields.LEVEL);
            level++;

            await node.SetFieldInt(target_id, Player.Fields.LEVEL, level);
        }

        private static async Task OnPlayerCreate(INode node, Nuid id, INList args)
        {
            await node.SetFieldInt(id, Player.Fields.LEVEL, 1100);

            await node.SetFieldString(id, "nick_name", "1sadasdasd");

            await node.AddHeartbeat(id, "test", 10000, 10, OnHeartbeat);

            await node.AddKeyValue(id, Player.Tables.QuestTable.TABLE_NAME, 1001, NList.New().Add(1).Add(TimeUtils.NowMilliseconds));

            await node.AddKeyValue(id, Player.Tables.QuestTable.TABLE_NAME, 2002, NList.New().Add(2).Add(TimeUtils.NowMilliseconds));

            await node.AddKeyValue(id, Player.Tables.QuestTable.TABLE_NAME, 3003, NList.New().Add(3).Add(TimeUtils.NowMilliseconds));

            await node.Create("item", id, NList.New().Add(20001).Add(1));

            await node.Create("item", id, NList.New().Add(20002).Add(2));

            await node.Create("item", id, NList.New().Add(20003).Add(3));

            await node.Create("item", id, NList.New().Add(20004).Add(4));

            await node.Create("item", id, NList.New().Add(20005).Add(5));

            await node.Create("item", id, NList.New().Add(20006).Add(6));

            int status = await node.GetColInt(id, Player.Tables.QuestTable.TABLE_NAME, 1001, Player.Tables.QuestTable.COL_STATUS);
        }

        private static async Task OnHeartbeat(INode node, Nuid id, INList args)
        {
            string timer_name = args.Get<string>(0);
            long now_ticks = args.Get<long>(1);
            int RemainBeatCount = args.Get<int>(2);

            await node.Error(string.Format("{0} beat {1}", timer_name, RemainBeatCount));
        }

        private static Task OnCountdown(INode node, Nuid id, INList args)
        {
            return Task.CompletedTask;
        }

        private static async Task OnLevelChange(INode node, Nuid id, INList args)
        {
            int level = await node.GetFieldInt(id, "level");
        }
    }
}
