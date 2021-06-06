using Game.Resources.Msg;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Nesh.Abstractions;
using Nesh.Abstractions.Agent;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Manager;
using Nesh.Abstractions.Storage.Database;
using Nesh.Abstractions.Utils;
using Nesh.Runtime.Service;
using Orleans;
using StackExchange.Redis.Extensions.Core.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Node
{
    public partial class Node : Grain, INode
    {
        private IAgent Agent { get; set; }

        private long Identity { get; set; }

        private bool Activated { get; set; }

        private ILogger _Logger { get; set; }

        private IDisposable _TimerObj { get; set; }

        private TimerManager TimerManager { get; set; }

        private EntityManager EntityManager { get; set; }

        private List<NList> BatchCahceList { get; set; }

        private IRedisCacheClient _CacheClient { get; set; }

        private IMongoClient _IMongoClient { get; set; }

        private IEntityDB EntityDB { get; set; }

        private NodeType NodeType { get; set; }

        private IIdGeneratorService IdGenerator { get; set; }

        public override async Task OnActivateAsync()
        {
            Identity = this.GetPrimaryKeyLong();
            _Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger("Node[" + Identity + "]");
            _CacheClient = ServiceProvider.GetService<IRedisCacheClient>();
            _IMongoClient = ServiceProvider.GetService<IMongoClient>();
            EntityDB = ServiceProvider.GetService<IEntityDB>();
            IdGenerator = ServiceProvider.GetService<IIdGeneratorService>();

            EntityManager = new EntityManager();
            TimerManager = new TimerManager(this);

            NodeType = await EntityDB.GetNodeType(Identity);

            BatchCahceList = new List<NList>();

            RegisterTimer(ScheduledSave, null, TimeSpan.FromSeconds(0), TimeSpan.FromMilliseconds(TimeUtils.MINITE));

            bool persist = await EntityDB.IsPersist(Identity);
            if (persist)
            {
                IReadOnlyList<Entity> entities = await LoadPersistEntities();

                if (NodeType == NodeType.Grain)
                {
                    foreach (Entity entity in entities)
                    {
                        await Load(entity);
                    }
                }
                else if (NodeType == NodeType.Cache)
                {
                    if (!await CacheExist(Nuid.New(Identity, Identity)))
                    {
                        await SetCacheEntities(entities);
                    }
                }
            }

            await base.OnActivateAsync();
        }


        public override async Task OnDeactivateAsync()
        {
            await SavePersistEntities();

            await base.OnDeactivateAsync();
        }

        public Task<bool> IsActive()
        {
            return Task.FromResult(Activated);
        }

        public async Task Active()
        {
            IReadOnlyList<Entity> entities = EntityManager.GetEntities();
            foreach (Entity entity in entities)
            {
                entity.Activated = true;
            }

            foreach (Entity entity in entities)
            {
                await Entry(entity.Id);
            }

            Activated = true;
        }

        public async Task Deactive()
        {
            Activated = false;
            await SavePersistEntities();
        }

        public Task BindAgent(IAgent agent)
        {
            Agent = agent;

            return Task.CompletedTask;
        }

        private async Task ScheduledSave(object arg)
        {
            if (!Activated) return;
            await SavePersistEntities();
        }

        private async Task SyncEntity(Nuid id, NList args)
        {
            if (!Activated) return;

            Entity entity = EntityManager.Get(id);
            if (entity == null)
            {
                return;
            }

            if (!entity.Activated)
            {
                return;
            }

            await Agent.SendMessage(NList.New().Add(SystemMsg.SERVER.SYNC_ENTITY).Append(args));
        }

        private async Task SyncField(Nuid id, NList args)
        {
            if (!Activated) return;

            Entity entity = EntityManager.Get(id);
            if (entity == null)
            {
                return;
            }

            if (!entity.Activated)
            {
                return;
            }

            await Agent.SendMessage(NList.New().Add(SystemMsg.SERVER.SYNC_FIELD).Append(args));
        }

        private async Task SyncTable(Nuid id, NList args)
        {
            if (!Activated) return;

            Entity entity = EntityManager.Get(id);
            if (entity == null)
            {
                return;
            }

            if (!entity.Activated)
            {
                return;
            }

            await Agent.SendMessage(NList.New().Add(SystemMsg.SERVER.SYNC_TABLE).Append(args));
        }

        private async Task CallbackEntity(Nuid id, EntityEvent entity_event, NList args)
        {
            string entity_type = Global.NULL_STRING;
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                entity_type = entity.Type;
            }
            else
            {
                entity_type = await GetCacheType(id);
            }

            EntityPrefab entity_prefab = Prefabs.GetEntity(entity_type);
            if (entity_prefab == null)
            {
                return;
            }

            if (entity_prefab.ancestors != null && entity_prefab.ancestors.Count > 0)
            {
                for (int i = entity_prefab.ancestors.Count - 1; i >= 0; i--)
                {
                    string parent_type = entity_prefab.ancestors[i];

                    await NModule.CallbackEntity(this, id, parent_type, entity_event, args);
                }
            }

            await NModule.CallbackEntity(this, id, entity_type, entity_event, args);
        }

        private async Task CallbackField(Nuid id, string field_name, FieldEvent field_event, NList args)
        {
            string entity_type = Global.NULL_STRING;
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                entity_type = entity.Type;
            }
            else
            {
                entity_type = await GetCacheType(id);
            }

            EntityPrefab entity_prefab = Prefabs.GetEntity(entity_type);
            if (entity_prefab == null)
            {
                return;
            }

            if (entity_prefab.ancestors != null && entity_prefab.ancestors.Count > 0)
            {
                for (int i = entity_prefab.ancestors.Count - 1; i >= 0; i--)
                {
                    string parent_type = entity_prefab.ancestors[i];

                    await NModule.CallbackField(this, id, parent_type, field_name, field_event, args);
                }
            }

            await NModule.CallbackField(this, id, entity_type, field_name, field_event, args);

            FieldPrefab field_prefab = entity_prefab.fields[field_name];
            if (field_prefab != null && field_prefab.sync)
            {
                NList msg = NList.New().Add(id).Add(field_name).Add((int)field_event).Append(args);
                await SyncField(id, msg);
            }
        }

        private async Task CallbackTable(Nuid id, string table_name, TableEvent table_event, NList args)
        {
            string entity_type = Global.NULL_STRING;
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                entity_type = entity.Type;
            }
            else
            {
                entity_type = await GetCacheType(id);
            }

            EntityPrefab entity_prefab = Prefabs.GetEntity(entity_type);
            if (entity_prefab == null)
            {
                return;
            }

            if (entity_prefab.ancestors != null && entity_prefab.ancestors.Count > 0)
            {
                for (int i = entity_prefab.ancestors.Count - 1; i >= 0; i--)
                {
                    string parent_type = entity_prefab.ancestors[i];

                    await NModule.CallbackTable(this, id, parent_type, table_name, table_event, args);
                }
            }

            await NModule.CallbackTable(this, id, entity_type, table_name, table_event, args);

            TablePrefab table_prefab = entity_prefab.tables[table_name];
            if (table_prefab != null && table_prefab.sync)
            {
                NList msg = NList.New().Add(id).Add(table_name).Add((int)table_event).Append(args);
                await SyncTable(id, msg);
            }
        }

        public async Task Command(Nuid id, int command, NList msg)
        {
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                await NModule.CallbackCommand(this, id, command, msg);
            }
            else
            {
                if (id.Origin == Identity)
                {
                    await NModule.CallbackCommand(this, id, command, msg);
                }
                else
                {
                    INode node = GrainFactory.GetGrain<INode>(id.Origin);
                    await node.Command(id, command, msg);
                }
            }
        }

        public async Task Custom(Nuid id, int custom, NList msg)
        {
            Entity entity = EntityManager.Get(id);
            if (entity != null)
            {
                await NModule.CallbackCustom(this, id, custom, msg);
            }
            else
            {
                if (id.Origin == Identity) return;

                INode node = GrainFactory.GetGrain<INode>(id.Origin);

                if (await node.IsActive())
                {
                    await node.Custom(id, custom, msg);
                }
            }
        }

        public async Task<INode> CreateNode<AGENT>() where AGENT : IAgent
        {
            long identity = IdGenerator.NewIdentity();

            AGENT agent = GrainFactory.GetGrain<AGENT>(identity);

            await agent.Online();

            return GrainFactory.GetGrain<INode>(identity);
        }
    }
}
