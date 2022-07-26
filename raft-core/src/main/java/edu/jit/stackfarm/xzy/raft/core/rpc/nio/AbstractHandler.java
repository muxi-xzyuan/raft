package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class AbstractHandler extends ChannelDuplexHandler {

    protected final EventBus eventBus;
    protected Channel channel;
    NodeId remoteId;
    // string: message id
    LoadingCache<String, AppendEntriesRpc> appendEntriesRpcCache;
    private InstallSnapshotRpc lastInstallSnapshotRpc;

    AbstractHandler(EventBus eventBus) {
        this.eventBus = eventBus;
        appendEntriesRpcCache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(Duration.ofSeconds(10))
                .recordStats()
                .build(new CacheLoader<String, AppendEntriesRpc>() {
                    @Override
                    public AppendEntriesRpc load(String key) {
                        return null;
                    }
                });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert remoteId != null;
        assert channel != null;
        if (msg instanceof RequestVoteRpc) {
            //收到请求投票的消息
            eventBus.post(new RequestVoteRpcMessage((RequestVoteRpc) msg, remoteId, channel));
        } else if (msg instanceof PreVoteRpc) {
            eventBus.post(new PreVoteRpcMessage((PreVoteRpc) msg, remoteId, channel));
        } else if (msg instanceof AppendEntriesRpc) {
            //收到追加日志的请求
            eventBus.post(new AppendEntriesRpcMessage((AppendEntriesRpc) msg, remoteId, channel));
        } else if (msg instanceof AppendEntriesResult) {
            //收到追加日志的结果
            AppendEntriesResult result = (AppendEntriesResult) msg;
            AppendEntriesRpc rpc = appendEntriesRpcCache.get(result.getMessageId());
            if (rpc == null) {
                log.warn("{} corresponding rpc has expired, ignore.", result);
            } else {
                eventBus.post(new AppendEntriesResultMessage(result, remoteId, rpc));
            }
        } else if (msg instanceof InstallSnapshotRpc) {
            InstallSnapshotRpc rpc = (InstallSnapshotRpc) msg;
            eventBus.post(new InstallSnapshotRpcMessage(rpc, remoteId, channel));
        } else if (msg instanceof InstallSnapshotResult) {
            InstallSnapshotResult result = (InstallSnapshotResult) msg;
            assert lastInstallSnapshotRpc != null;
            eventBus.post(new InstallSnapshotResultMessage(result, remoteId, lastInstallSnapshotRpc));
            lastInstallSnapshotRpc = null;
        } else {
            // RequestVoteResult
            // PreVoteResult
            eventBus.post(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof AppendEntriesRpc) {
            appendEntriesRpcCache.put(((AppendEntriesRpc) msg).getMessageId(), (AppendEntriesRpc) msg);
        } else if (msg instanceof InstallSnapshotRpc) {
            lastInstallSnapshotRpc = (InstallSnapshotRpc) msg;
        }
        super.write(ctx, msg, promise);
    }


}