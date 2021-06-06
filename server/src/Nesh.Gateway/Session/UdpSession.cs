using LiteNetLib;
using LiteNetLib.Utils;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using Orleans;
using System.Threading.Tasks;

namespace Nesh.Gateway.Session
{
    class UdpSession : GameSession
    {
        private NetPeer Connection { get; set; }
        public UdpSession(NetPeer peer, IClusterClient client, ILoggerFactory loggerFactory) :
            base(Protocol.UdpSocket, client, loggerFactory)
        {
            Connection = peer;
        }

        public override Task OutcomingMessage(NList message)
        {
            byte[] data = ProtoUtils.Serialize(message);
            NetDataWriter netDataWriter = new NetDataWriter();
            netDataWriter.PutBytesWithLength(data);
            Connection.Send(netDataWriter, DeliveryMethod.ReliableOrdered);

            return Task.CompletedTask;
        }

        internal override void Close()
        {
            Connection.Disconnect();
            base.Close();
        }
    }
}
