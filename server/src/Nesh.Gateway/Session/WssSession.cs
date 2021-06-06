using DotNetty.Codecs.Http.WebSockets;
using DotNetty.Transport.Channels;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using Orleans;
using System.Threading.Tasks;

namespace Nesh.Gateway.Session
{
    class WssSession : GameSession
    {
        private IChannelHandlerContext _Context { get; }
        public WssSession(IClusterClient client, ILoggerFactory loggerFactory, IChannelHandlerContext channelHandlerContext) :
            base(Protocol.WebSocket, client, loggerFactory)
        {
            _Context = channelHandlerContext;
        }

        public override async Task OutcomingMessage(NList message)
        {
            string data = JsonUtils.ToJson(message);

            await _Context.Channel.WriteAndFlushAsync(new TextWebSocketFrame(data));
        }

        internal override void Close()
        {
            _Context.CloseAsync();
            base.Close();
        }
    }
}