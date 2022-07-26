package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ToRemoteHandler extends AbstractHandler {

    private final NodeId selfNodeId;

    ToRemoteHandler(EventBus eventBus, NodeId remoteId, NodeId selfNodeId) {
        super(eventBus);
        this.remoteId = remoteId;
        this.selfNodeId = selfNodeId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.write(selfNodeId);
        channel = ctx.channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.debug("Receive {} from {}", msg, remoteId);
        super.channelRead(ctx, msg);
    }

}