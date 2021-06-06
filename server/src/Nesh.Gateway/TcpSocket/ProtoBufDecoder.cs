using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using Nesh.Abstractions.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace Nesh.Gateway.TcpSocket
{
    public sealed class ProtoBufDecoder<T> : MessageToMessageDecoder<IByteBuffer>
    {
        protected override void Decode(IChannelHandlerContext context, IByteBuffer message, List<object> output)
        {
            Contract.Requires(context != null);
            Contract.Requires(message != null);
            Contract.Requires(output != null);
            Contract.Requires(message.ReadableBytes > 0);

            try
            {
                Span<byte> spby = ((Span<byte>)message.Array).Slice(message.ArrayOffset, message.ReadableBytes);

                T decoded = ProtoUtils.Deserialize<T>(spby.ToArray());

                if (decoded != null)
                    output.Add(decoded);
            }
            catch (Exception exception)
            {
                throw new CodecException(exception);
            }
        }
    }
}
