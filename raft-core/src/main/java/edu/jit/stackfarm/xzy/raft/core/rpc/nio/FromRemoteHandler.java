package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class FromRemoteHandler extends AbstractHandler {

    private final InboundChannelGroup channelGroup;

    FromRemoteHandler(EventBus eventBus, InboundChannelGroup channelGroup) {
        super(eventBus);
        this.channelGroup = channelGroup;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof NodeId) {
            remoteId = (NodeId) msg;
            Channel channel = ctx.channel();
            this.channel = channel;
            channelGroup.add(remoteId, channel);
            return;
        }
        log.debug("Receive {} from {}", msg, remoteId);
        super.channelRead(ctx, msg);
    }

}
