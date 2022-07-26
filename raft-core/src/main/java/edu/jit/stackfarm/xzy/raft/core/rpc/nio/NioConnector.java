package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.ChannelConnectException;
import edu.jit.stackfarm.xzy.raft.core.rpc.Connector;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AbstractRpcMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class NioConnector implements Connector {
    //Selector线程池，此处为单线程
    private final NioEventLoopGroup bossNioEventLoopGroup = new NioEventLoopGroup(1);
    //IO线程池，此处为固定数量多线程
    private final NioEventLoopGroup workerNioEventLoopGroup;
    //是否和上层服务等共享IO线程池
    private final boolean workerGroupShared;
    private final EventBus eventBus;
    private final int port;
    private final InboundChannelGroup inboundChannelGroup = new InboundChannelGroup();
    private final OutBoundChannelGroup outBoundChannelGroup;
    //用于并行发起连接的线程池
    //由于发起连接属于耗时操作，需要从主线程中分离出来
    private final ExecutorService executorService = Executors.newCachedThreadPool((r) -> {
        Thread thread = new Thread(r);
        thread.setUncaughtExceptionHandler((t, e) -> logException(e));
        return thread;
    });

    public NioConnector(NodeId selfNodeId, EventBus eventBus, int port) {
        this(new NioEventLoopGroup(), false, selfNodeId, eventBus, port);
    }

    public NioConnector(NioEventLoopGroup workerNioEventLoopGroup, NodeId selfNodeId, EventBus eventBus, int port) {
        this(workerNioEventLoopGroup, true, selfNodeId, eventBus, port);
    }

    public NioConnector(
            NioEventLoopGroup workerNioEventLoopGroup,
            boolean workerGroupShared,
            NodeId selfNodeId,
            EventBus eventBus,
            int port
    ) {
        this.workerNioEventLoopGroup = workerNioEventLoopGroup;
        this.workerGroupShared = workerGroupShared;
        this.eventBus = eventBus;
        this.port = port;
        this.outBoundChannelGroup = new OutBoundChannelGroup(workerNioEventLoopGroup, eventBus, selfNodeId);
    }

    @Override
    public void initialize() {
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(bossNioEventLoopGroup, workerNioEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ProtobufDecoder());
                        pipeline.addLast(new ProtobufEncoder());
                        pipeline.addLast(new FromRemoteHandler(eventBus, inboundChannelGroup));
                    }
                });
        log.debug("Raft node tries to listen on port {}", port);
        try {
            serverBootstrap.bind(port).sync();
        } catch (InterruptedException e) {
            throw new ConnectorException("Failed to bind port", e);
        }
    }

    @Override
    public void send(@NonNull Object rpc, @NonNull NodeEndpoint targetEndpoint) {
        log.debug("Send {} to node {}", rpc, targetEndpoint.getId());
        executorService.execute(() -> getChannel(targetEndpoint).writeAndFlush(rpc));
    }

    @Override
    public void send(@NonNull Object rpc, @NonNull Collection<NodeEndpoint> targetEndpoints) {
        for (NodeEndpoint endpoint : targetEndpoints) {
            log.debug("Send {} to node {}", rpc, endpoint.getId());
            executorService.execute(() -> getChannel(endpoint).writeAndFlush(rpc));
        }
    }

    @Override
    public void reply(@NonNull Object result, @NonNull AbstractRpcMessage<?> rpcMessage) {
        log.debug("Reply {} to node {}", result, rpcMessage.getSourceNodeId());
        executorService.execute(() -> rpcMessage.getChannel().writeAndFlush(result));
    }

    @Override
    public void close() {
        log.debug("Closing connector");
        executorService.shutdown();
        inboundChannelGroup.closeAll();
        outBoundChannelGroup.closeAll();
        bossNioEventLoopGroup.shutdownGracefully();
        if (!workerGroupShared) {
            workerNioEventLoopGroup.shutdownGracefully();
        }
    }

    private Channel getChannel(NodeEndpoint endpoint) {
        return outBoundChannelGroup.getOrConnect(endpoint, endpoint.getAddress());
    }

    private void logException(Throwable throwable) {
        if (throwable instanceof Error) {
            throwable = throwable.getCause();
        }
        if (throwable instanceof ChannelConnectException || throwable instanceof SocketException) {
            log.warn(throwable.getMessage());
        } else {
            log.warn("Failed to process channel", throwable);
        }
    }

}
