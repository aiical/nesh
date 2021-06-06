using DotNetty.Transport.Channels;
using Nesh.Gateway.Session;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Orleans;
using System;

namespace Nesh.Gateway.TcpSocket
{
    class TcpSocketChannelHandler : SimpleChannelInboundHandler<NList>
    {
        private readonly ILogger _Logger;
        private readonly IClusterClient _ClusterClient;
        private readonly ILoggerFactory _LoggerFactory;
        private readonly IConfiguration _Configuration;
        private TcpSession _Session;

        public TcpSocketChannelHandler(IClusterClient client, ILoggerFactory loggerFactory, IConfiguration configuration)
        {
            _ClusterClient = client;
            _LoggerFactory = loggerFactory;
            _Logger = loggerFactory.CreateLogger<TcpSocketChannelHandler>();
            _Configuration = configuration;
        }

        protected override async void ChannelRead0(IChannelHandlerContext ctx, NList msg)
        {
            _Logger.LogInformation($"TcpSession recv {msg}");
             await _Session.IncomingMessage(msg);
        }

        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
            context.Flush();
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            _Logger.LogError($"Gateway Exception: {exception}");
            context.CloseAsync();
        }

        public override void ChannelRegistered(IChannelHandlerContext context)
        {
            _Session = new TcpSession(_ClusterClient, _LoggerFactory, context);
            base.ChannelRegistered(context);
        }

        public override void ChannelUnregistered(IChannelHandlerContext context)
        {
            _Session.Close();
            base.ChannelUnregistered(context);
        }
    }
}
