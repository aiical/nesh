using DotNetty.Buffers;
using DotNetty.Transport.Channels;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Nesh.Abstractions.Utils;
using Orleans;
using System;
using System.Threading.Tasks;

namespace Nesh.Gateway.Session
{
    class TcpSession : GameSession
    {
        private IChannelHandlerContext _Context { get; }
        public TcpSession(IClusterClient client, ILoggerFactory loggerFactory, IChannelHandlerContext channelHandlerContext) :
            base(Protocol.TcpSocket, client, loggerFactory)
        {
            _Context = channelHandlerContext;
        }

        public override async Task OutcomingMessage(NList send_msg)
        {
            try
            {
                if (_Context.Channel.IsActive)
                {
                    var bytes = ProtoUtils.Serialize(send_msg);
                    IByteBuffer buffer = Unpooled.WrappedBuffer(bytes);
                    await _Context.WriteAndFlushAsync(buffer);
                }
            }
            catch (Exception ex)
            {
                _Logger.LogError(
                    $"OutcomingMessage异常:\n" +
                    $"{ex.Message}\n" +
                    $"{ex.StackTrace}\n" +
                    $"{send_msg}");
            }
        }

        internal override void Close()
        {
            _Context.CloseAsync();
            base.Close();
        }
    }
}
