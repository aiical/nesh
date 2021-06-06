using Nesh.Gateway.Session;
using LiteNetLib;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using Orleans;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Nesh.Gateway.UdpSocket
{
    class UdpSocketService : IHostedService
    {
        private readonly IClusterClient _ClusterClient;
        private readonly IConfiguration _Configuration;
        private readonly ILoggerFactory _LoggerFactory;
        private readonly ILogger _Logger;
        private readonly EventBasedNetListener _Listener;
        private readonly NetManager _NetManager;
        private Dictionary<int, UdpSession> _Sessions;

        public UdpSocketService(IClusterClient client, IConfiguration config, ILoggerFactory loggerFactory)
        {
            _ClusterClient = client;
            _Configuration = config;
            _LoggerFactory = loggerFactory;
            _Logger = _LoggerFactory.CreateLogger<UdpSocketService>();
            _Sessions = new Dictionary<int, UdpSession>();

            _Listener = new EventBasedNetListener();
            _NetManager = new NetManager(_Listener);
            _NetManager.UpdateTime = 30;
            _NetManager.IPv6Enabled = IPv6Mode.DualMode;
            _NetManager.UnsyncedEvents = true;
            _NetManager.DisconnectTimeout = 60000;

            _Listener.PeerConnectedEvent += OnPeerConnected;
            _Listener.PeerDisconnectedEvent += OnPeerDisconnected;
            _Listener.NetworkReceiveEvent += OnNetworkReceive;
            _Listener.NetworkErrorEvent += OnNetworkError;
            _Listener.ConnectionRequestEvent += OnConnectionRequest;
        }

        private void OnConnectionRequest(ConnectionRequest request)
        {
            _Logger.LogInformation(request.Data.GetString());
            request.Accept();
        }

        private async void OnNetworkReceive(NetPeer peer, NetPacketReader reader, DeliveryMethod clientDeliveryMethod)
        {
            NList msg = ProtoUtils.Deserialize<NList>(reader.GetBytesWithLength());
            UdpSession session = null;
            if (_Sessions.TryGetValue(peer.Id, out session))
            {
                await session.IncomingMessage(msg);
            }

            _NetManager.TriggerUpdate();
            reader.Recycle();
        }

        private void OnNetworkError(IPEndPoint endpoint, SocketError socketerror)
        {
        }

        private void OnPeerConnected(NetPeer peer)
        {
            _Logger.LogInformation($"OnPeerConnected {peer.Id}");
            UdpSession session = new UdpSession(peer, _ClusterClient, _LoggerFactory);
            _Sessions.Add(peer.Id, session);
        }

        private void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectinfo)
        {
            UdpSession session = null;
            if (_Sessions.TryGetValue(peer.Id, out session))
            {
                session.Close();
            }

            _Sessions.Remove(peer.Id);
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            int port = int.Parse(_Configuration.GetSection("UdpSocket")["port"]);

            _NetManager.Start(port);

            await Task.Run(() =>
            {
                while (true)
                {
                    _NetManager.PollEvents();
                }
            });
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _NetManager.Stop();

            return Task.CompletedTask;
        }
    }
}