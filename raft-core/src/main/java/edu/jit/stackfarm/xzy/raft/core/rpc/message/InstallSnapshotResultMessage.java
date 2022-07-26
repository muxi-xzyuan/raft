package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Getter;
import lombok.NonNull;

public class InstallSnapshotResultMessage {

    private final InstallSnapshotResult result;
    @Getter
    private final NodeId sourceNodeId;
    @Getter
    private final InstallSnapshotRpc rpc;

    public InstallSnapshotResultMessage(InstallSnapshotResult result, NodeId sourceNodeId, @NonNull InstallSnapshotRpc rpc) {
        this.result = result;
        this.sourceNodeId = sourceNodeId;
        this.rpc = rpc;
    }

    public InstallSnapshotResult getResult() {
        return result;
    }

}
