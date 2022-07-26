package edu.jit.stackfarm.xzy.raft.kvstore.message;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class CommandRequest<T> {

    @Getter
    private final T command;

    private final Channel channel;

    public void reply(Object response) {
        this.channel.writeAndFlush(response);
    }

    public void addCloseListener(Runnable runnable) {
        this.channel.closeFuture().addListener((ChannelFutureListener) future -> runnable.run());
    }

}