using Game.Resources.Entity;
using Nesh.Abstractions;
using Nesh.Abstractions.Data;
using System.Threading.Tasks;

namespace Game.Modules
{
    public class ItemModule : NModule
    {
        protected override void OnInit()
        {
            RCEntity(Item.TYPE, EntityEvent.OnCreate, OnItemCreate);
        }

        private async Task OnItemCreate(INode node, Nuid id, INList args)
        {
            int item_entry = args.Get<int>(0);
            int item_count = args.Get<int>(1);
            await node.SetFieldInt(id, Item.Fields.ENTRY, item_entry);
            await node.SetFieldInt(id, Item.Fields.COUNT, item_count);

            await node.AddKeyValue(id, Item.Tables.StarTable.TABLE_NAME, 1001, NList.New().Add(1));
            await node.AddKeyValue(id, Item.Tables.StarTable.TABLE_NAME, 101, NList.New().Add(1));
        }
    }
}
