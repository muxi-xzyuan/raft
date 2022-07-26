package edu.jit.stackfarm.xzy.raft.kvstore.server;

import edu.jit.stackfarm.xzy.raft.core.service.AddNodeCommand;
import edu.jit.stackfarm.xzy.raft.core.service.RemoveNodeCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.message.CommandRequest;
import edu.jit.stackfarm.xzy.raft.kvstore.message.GetCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.message.SetCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServiceHandler extends ChannelInboundHandlerAdapter {

    private final Service service;

    public ServiceHandler(Service service) {
        this.service = service;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof AddNodeCommand) {
            service.addNode(new CommandRequest<>((AddNodeCommand) msg, ctx.channel()));
        } else if (msg instanceof RemoveNodeCommand) {
            service.removeNode(new CommandRequest<>((RemoveNodeCommand) msg, ctx.channel()));
        } else if (msg instanceof GetCommand) {
            service.get(new CommandRequest<>((GetCommand) msg, ctx.channel()));
        } else if (msg instanceof SetCommand) {
            service.set(new CommandRequest<>((SetCommand) msg, ctx.channel()));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}