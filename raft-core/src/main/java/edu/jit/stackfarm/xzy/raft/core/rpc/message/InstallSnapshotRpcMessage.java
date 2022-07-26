package edu.jit.stackfarm.xzy.raft.core.rpc.message;


import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;

import javax.annotation.Nullable;

public class InstallSnapshotRpcMessage extends AbstractRpcMessage<InstallSnapshotRpc> {

    public InstallSnapshotRpcMessage(InstallSnapshotRpc rpc, NodeId sourceNodeId, @Nullable Channel channel) {
        super(rpc, sourceNodeId, channel);
    }

}
