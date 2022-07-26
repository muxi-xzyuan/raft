package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.Address;
import edu.jit.stackfarm.xzy.raft.core.rpc.ChannelConnectException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
class OutBoundChannelGroup {

    private final EventLoopGroup workerGroup;
    private final EventBus eventBus;
    private final NodeId selfNodeId;
    private final ConcurrentMap<NodeEndpoint, Channel> channelMap = new ConcurrentHashMap<>();

    OutBoundChannelGroup(EventLoopGroup workerGroup, EventBus eventBus, NodeId selfNodeId) {
        this.workerGroup = workerGroup;
        this.eventBus = eventBus;
        this.selfNodeId = selfNodeId;
    }

    Channel getOrConnect(NodeEndpoint nodeEndpoint, Address address) {
        Channel channel = channelMap.get(nodeEndpoint);
        if (channel == null) {
            try {
                channel = connect(nodeEndpoint, address);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return channel;
    }

    void closeAll() {
        log.debug("Close all outbound channels");
        channelMap.forEach((nodeId, channel) -> {
            try {
                channel.close();
            } catch (Exception e) {
                log.warn("Failed to close", e);
            }
        });
    }

    private Channel connect(NodeEndpoint nodeEndpoint, Address address) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ProtobufDecoder());
                        pipeline.addLast(new ProtobufEncoder());
                        pipeline.addLast(new ToRemoteHandler(eventBus, nodeEndpoint.getId(), selfNodeId));
                    }
                });
        ChannelFuture future;
        try {
            future = bootstrap.connect(address.getHost(), address.getPort()).sync();
        } catch (UnresolvedAddressException e) {
            throw new ChannelConnectException("Host <" + address.getHost() + "> cannot be resolved", e);
        }
        if (!future.isSuccess()) {
            throw new ChannelConnectException("Failed to connect", future.cause());
        }
        log.debug("Channel OUTBOUND-{} connected", nodeEndpoint);
        Channel channel = future.channel();
        channelMap.put(nodeEndpoint, channel);
        channel.closeFuture().addListener((ChannelFutureListener) cf -> {
            log.debug("Channel OUTBOUND-{} disconnected", nodeEndpoint);
            channelMap.remove(nodeEndpoint);
        });
        return channel;
    }
}
