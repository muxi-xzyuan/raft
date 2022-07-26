package edu.jit.stackfarm.xzy.raft.kvstore.server;

import edu.jit.stackfarm.xzy.raft.core.node.Node;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server {

    private final Node node;
    private final int port;
    private final Service service;
    private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(4);

    public Server(Node node, int port) {
        this.node = node;
        this.port = port;
        this.service = new Service(node);
    }

    public void start() {
        node.start();
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new Encoder());
                        pipeline.addLast(new Decoder());
                        pipeline.addLast(new ServiceHandler(service));
                    }
                });
        log.info("Server tries to listen on port {}", port);
        serverBootstrap.bind(port);
    }

    public void stop() {
        log.info("Stopping server");
        node.stop();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

}
