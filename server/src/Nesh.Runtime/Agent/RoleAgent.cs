using Game.Resources.Entity;
using Game.Resources.Msg;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions;
using Nesh.Abstractions.Agent;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Data;
using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Runtime.Agent
{
    public class RoleAgent : NodeAgent, IRoleAgent
    {
        private long Role { get; set; }
        private ILogger _Logger { get; set; }
        private IAsyncStream<NList> _ClientStream { get; set; }
        private Dictionary<int, Func<NList, Task>> _SystemListener { get; set; }
        private INode _RoleNode { get; set; }
        private Nuid _RoleId { get; set; }

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();

            _SystemListener = new Dictionary<int, Func<NList, Task>>();
            _Logger = ServiceProvider.GetService<ILoggerFactory>().CreateLogger("RoleAgent[" + Role + "]");

            Role = this.GetPrimaryKeyLong();
            _RoleNode = GrainFactory.GetGrain<INode>(Role);
            _RoleId = Nuid.New(Role, Role);

            if (!await EntityDB.IsPersist(Role))
            {
                await EntityDB.SetNodeType(Role, NodeType.Grain);

                await _RoleNode.Create(_RoleId, Player.TYPE, NList.New());
            }

            await _RoleNode.BindAgent(this);

            RegisterSystem(SystemMsg.CLIENT.CUSTOM, OnRecv_Custom);
        }

        private void RegisterSystem(int system_id, Func<NList, Task> callback)
        {
            if (_SystemListener.ContainsKey(system_id))
            {
                return;
            }

            _SystemListener.Add(system_id, callback);
        }

        public Task BindSession(Guid user_id, string stream)
        {
            _ClientStream = GetStreamProvider(StreamProviders.AgentProvider).GetStream<NList>(user_id, stream);

            return Task.CompletedTask;
        }

        public override async Task SendMessage(NList message)
        {
            await _ClientStream.OnNextAsync(message);
        }

        public override async Task OnResponse(int message_id, NList message)
        {
            try
            {
                Func<NList, Task> found = null;
                if (_SystemListener.TryGetValue(message_id, out found))
                {
                    await found(message);
                }
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "OnResponse message_id={0} message={1}", message_id, message);
            }
        }

        private async Task OnRecv_Custom(NList msg)
        {
            int custom_msg = msg.Get<int>(0);
            NList args = msg.GetRange(1, msg.Count-1);

            await NModule.CallbackCustom(_RoleNode, _RoleId, custom_msg, args);
        }

        public async Task SendEntities()
        {
            NList entities = await _RoleNode.GetEntities();

            NList message = NList.New().Add(SystemMsg.SERVER.LOAD_ENTITIES).Add(Prefabs.JSON).Add(_RoleId).Add(entities);

            await SendMessage(message);
        }

        public override async Task Online()
        {
            bool actived = await _RoleNode.IsActive();
            if (!actived)
            {
                await _RoleNode.Active();
            }
        }

        public override async Task Offline()
        {
            bool actived = await _RoleNode.IsActive();
            if (actived)
            {
                await _RoleNode.Deactive();
            }
        }
    }
}
