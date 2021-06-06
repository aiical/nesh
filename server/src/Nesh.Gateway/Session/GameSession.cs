using Game.Resources.Msg;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Agent;
using Nesh.Abstractions.Auth;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Storage.Models;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nesh.Gateway.Session
{
    public enum Protocol
    {
        TcpSocket = 1,
        UdpSocket = 2,
        WebSocket = 3,
    }

    abstract class GameSession
    {
        protected IClusterClient _ClusterClient;
        protected ILogger _Logger;
        protected IAgentObserver _AgentObserver;
        protected IUserAccount _PlatformAccount;
        protected StreamSubscriptionHandle<NList> _SubscriptionHandle;
        protected Protocol Protocol;
        protected IRoleAgent RoleAgent;

        protected bool Inited { get; set; }
        protected bool Access { get; set; }

        public GameSession(Protocol protocol, IClusterClient client, ILoggerFactory loggerFactory)
        {
            Protocol = protocol;
            _ClusterClient = client;
            _Logger = loggerFactory.CreateLogger<GameSession>();
            Inited = false;
        }

        public abstract Task OutcomingMessage(NList message);

        internal async Task IncomingMessage(NList message)
        {
            try
            {
                int message_id = message.Get<int>(0);
                NList dispatch_msg = message.GetRange(1, message.Count - 1);

                if (message_id == SystemMsg.CLIENT.ACCESS_TOKEN && !Access)
                {
                    string access_token = dispatch_msg.Get<string>(0);
                    IAccessToken token = _ClusterClient.GetGrain<IAccessToken>(access_token);
                    Guid user_id = await token.GetUserId();
                    if (user_id == Guid.Empty)
                    {
                        await OutcomingMessage(NList.New().Add(SystemMsg.SERVER.ACCESS_TOKEN).Add(false));
                        return;
                    }

                    _PlatformAccount = _ClusterClient.GetGrain<IUserAccount>(user_id);
                    IReadOnlyList<Role> roles = await _PlatformAccount.GetRoles();
                    if (roles == null)
                    {
                        await OutcomingMessage(NList.New().Add(SystemMsg.SERVER.ACCESS_TOKEN).Add(false));
                        return;
                    }

                    Access = true;
                    await OutcomingMessage(NList.New().Add(SystemMsg.SERVER.ACCESS_TOKEN).Add(true).Add(JsonConvert.SerializeObject(roles)));
                }
                else if (message_id == SystemMsg.CLIENT.CREATE_ROLE && Access)
                {
                    int realm = dispatch_msg.Get<int>(0);
                    string json = dispatch_msg.Get<string>(1);
                    Role role = await _PlatformAccount.CreateRole(realm, json);
                    if (role != null)
                    {
                        await OutcomingMessage(NList.New().Add(SystemMsg.SERVER.CREATE_ROLE).Add(true).Add(JsonConvert.SerializeObject(role)));
                    }
                    else
                    {
                        await OutcomingMessage(NList.New().Add(SystemMsg.SERVER.CREATE_ROLE).Add(false));
                    }
                }
                else if (message_id == SystemMsg.CLIENT.SELECT_ROLE && Access && !Inited)
                {
                    long role_id = dispatch_msg.Get<long>(0);
                    if (!await _PlatformAccount.HasRole(role_id))
                    {
                        return;
                    }

                    RoleAgent = _ClusterClient.GetGrain<IRoleAgent>(role_id);
                    await RoleAgent.BindSession(_PlatformAccount.GetPrimaryKey(), Protocol.ToString());
                    _AgentObserver = new AgentObserver(this);
                    var stream = _ClusterClient.GetStreamProvider(StreamProviders.AgentProvider).GetStream<NList>(_PlatformAccount.GetPrimaryKey(), Protocol.ToString());
                    _SubscriptionHandle = await stream.SubscribeAsync(_AgentObserver);

                    await RoleAgent.SendEntities();

                    Inited = true;
                }
                else if (message_id == SystemMsg.CLIENT.ONLINE && Inited)
                {
                    if (RoleAgent != null)
                    {
                        await RoleAgent.Online();
                    }
                }
                else if (Inited)
                {
                    if (RoleAgent != null)
                    {
                        await RoleAgent.OnResponse(message_id, dispatch_msg);
                    }
                }
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "IncomingMessage Failed!");
            }
        }

        internal virtual void Close()
        {
            if (_SubscriptionHandle != null) _SubscriptionHandle.UnsubscribeAsync();
        }
    }

    class AgentObserver : IAgentObserver
    {
        private GameSession _Session;

        public AgentObserver(GameSession session)
        {
            _Session = session;
        }

        public Task OnCompletedAsync()
        {
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            return Task.CompletedTask;
        }

        public Task OnNextAsync(NList msg, StreamSequenceToken token = null)
        {
            if (_Session != null)
            {
                _Session.OutcomingMessage(msg);
            }
            return Task.CompletedTask;
        }
    }
}
