using DotNetty.Codecs;
using DotNetty.Handlers.Timeout;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Libuv;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Nesh.Abstractions.Data;
using Orleans;
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Nesh.Gateway.TcpSocket
{
    public class TcpSocketService : IHostedService
    {
        private readonly IClusterClient _client;
        private readonly IConfiguration _config;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;

        IEventLoopGroup _bossGroup;
        IEventLoopGroup _workerGroup;
        ServerBootstrap _bootstrap = new ServerBootstrap();
        IChannel _bootstrapChannel = null;

        public TcpSocketService(
            IClusterClient client,
            IConfiguration config,
            ILoggerFactory loggerFactory)
        {
            _client = client;
            _config = config;
            _loggerFactory = loggerFactory;
            _logger = _loggerFactory.CreateLogger<TcpSocketService>();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            IPAddress ip_address = IPAddress.Parse(_config.GetSection("TcpSocket")["address"]);
            int port = int.Parse(_config.GetSection("TcpSocket")["port"]);
            var dispatcher = new DispatcherEventLoopGroup();
            _bossGroup = dispatcher;
            _workerGroup = new WorkerEventLoopGroup(dispatcher);

            _bootstrap.Group(_bossGroup, _workerGroup);
            _bootstrap.Channel<TcpServerChannel>();

            _bootstrap
                .Option(ChannelOption.SoBacklog, 100)
                .Option(ChannelOption.TcpNodelay, true)
                //.Handler(new LoggingHandler("SRV-LSTN"))
                .ChildHandler(new ActionChannelInitializer<IChannel>(channel =>
                {
                    IChannelPipeline pipeline = channel.Pipeline;

                    pipeline.AddLast("framing-dec", new LengthFieldBasedFrameDecoder(int.MaxValue, 0, 4, 0, 4));
                    pipeline.AddLast("decoder", new ProtoBufDecoder<NList>());
                    pipeline.AddLast("framing-enc", new LengthFieldPrepender(4));
                    pipeline.AddLast("encoder", new ProtoBufEncoder<NList>());

                    pipeline.AddLast("ping", new IdleStateHandler(0, 10, 0));
                    
                    pipeline.AddLast(new TcpSocketChannelHandler(_client, _loggerFactory, _config));
                }));

            _bootstrapChannel = await _bootstrap.BindAsync(port);
            _logger.LogInformation($"TcpSocketService Startup {ip_address}:{port}");
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                await _bootstrapChannel.CloseAsync();
                _logger.LogInformation($"TcpSocketService Shutdown");
            }
            finally
            {
                await Task.WhenAll(
                    _bossGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)),
                    _workerGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)));
            }
        }
    }
}
