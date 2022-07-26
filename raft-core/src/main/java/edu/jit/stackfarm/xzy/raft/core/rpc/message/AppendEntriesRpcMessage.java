package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;

public class AppendEntriesRpcMessage extends AbstractRpcMessage<AppendEntriesRpc> {

    public AppendEntriesRpcMessage(AppendEntriesRpc rpc, NodeId sourceNodeId, Channel channel) {
        super(rpc, sourceNodeId, channel);
    }

}

