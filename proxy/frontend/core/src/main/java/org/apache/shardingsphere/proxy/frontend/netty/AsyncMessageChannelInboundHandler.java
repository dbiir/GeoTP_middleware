package org.apache.shardingsphere.proxy.frontend.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.transactions.AgentAsyncXAManager;
import org.apache.shardingsphere.infra.transactions.AsyncMessageFromAgent;
import org.apache.shardingsphere.infra.transactions.CustomXID;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Frontend channel inbound handler.
 */
@Slf4j
public final class AsyncMessageChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static ChannelHandlerContext context;
    private AsyncMessageFromAgent asyncMessageFromAgent = null;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        context = ctx;
    }

    public static void sendMessage(String message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(message).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    public static void sendMessage(byte[] message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(Unpooled.wrappedBuffer(message)).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    public static void sendMessage(ByteBuf message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(message).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AsyncMessageFromAgent) {
            AsyncMessageFromAgent message = (AsyncMessageFromAgent) msg;
            System.out.println("after decoder message: " + message);
            CustomXID xidFromMessage = new CustomXID(message.getXid());
            AgentAsyncXAManager.getInstance().setStateByXid(xidFromMessage, message.getState());
            if (!message.getSQLExceptionString().equals("")) {
                AgentAsyncXAManager.getInstance().setErrorInfoByXid(xidFromMessage, message.getSQLExceptionString());
            }
        } else if (msg instanceof ByteBuf) {
            ByteBuf byteBuf = (ByteBuf) msg;

            byte[] out = new byte[byteBuf.readableBytes()];
            byteBuf.readBytes(byteBuf.readableBytes()); // read one message
            String content = new String(out, StandardCharsets.UTF_8);

            try {
                ObjectMapper mapper = new ObjectMapper();
                AsyncMessageFromAgent message = mapper.readValue(content, AsyncMessageFromAgent.class);
                System.out.println("receive message: " + message.toString());

                CustomXID xidFromMessage = new CustomXID(message.getXid());
                AgentAsyncXAManager.getInstance().setStateByXid(xidFromMessage, message.getState());
                if (!message.getSQLExceptionString().equals("")) {
                    AgentAsyncXAManager.getInstance().setErrorInfoByXid(xidFromMessage, message.getSQLExceptionString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
