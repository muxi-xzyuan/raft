package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
class InboundChannelGroup {

    private final List<Channel> channels = new CopyOnWriteArrayList<>();

    public void add(NodeId remoteId, Channel channel) {
        log.debug("Channel INBOUND-{} connected", remoteId);
        channel.closeFuture().addListener(future -> {
            log.debug("Channel INBOUND-{} disconnected", remoteId);
            remove(channel);
        });
    }

    void closeAll() {
        log.debug("Close all inbound channels");
        for (Channel channel : channels) {
            channel.close();
        }
    }

    private void remove(Channel channel) {
        channels.remove(channel);
    }
}
