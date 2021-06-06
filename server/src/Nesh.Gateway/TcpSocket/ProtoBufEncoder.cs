using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using Nesh.Abstractions.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace Nesh.Gateway.TcpSocket
{
    class ProtoBufEncoder<T> : MessageToMessageEncoder<T>
    {
        protected override void Encode(IChannelHandlerContext context, T message, List<object> output)
        {
            Contract.Requires(context != null);
            Contract.Requires(message != null);
            Contract.Requires(output != null);

            IByteBuffer buffer = null;

            try
            {
                buffer = Unpooled.WrappedBuffer(ProtoUtils.Serialize(message));
                output.Add(buffer);
                buffer = null;
            }
            catch (Exception exception)
            {
                throw new CodecException(exception);
            }
            finally
            {
                buffer?.Release();
            }
        }
    }
}
