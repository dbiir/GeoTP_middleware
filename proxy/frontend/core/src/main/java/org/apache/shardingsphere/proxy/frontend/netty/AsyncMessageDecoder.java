package org.apache.shardingsphere.proxy.frontend.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.shardingsphere.infra.transactions.AsyncMessageFromAgent;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class AsyncMessageDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> out) throws Exception {
        if (byteBuf.readableBytes() < 4) {
            return; // Insufficient message length information, waiting for more data
        }

        byteBuf.markReaderIndex(); // mark current position

        int contentLength = byteBuf.readInt(); // read message length
        if (byteBuf.readableBytes() < contentLength) {
            byteBuf.resetReaderIndex();
            return; // wait for more message
        }

        byte[] contentBytes = new byte[contentLength];
        byteBuf.readBytes(contentBytes); // read one message
        String content = new String(contentBytes, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        AsyncMessageFromAgent message = mapper.readValue(content, AsyncMessageFromAgent.class);

        out.add(message);
    }
}

