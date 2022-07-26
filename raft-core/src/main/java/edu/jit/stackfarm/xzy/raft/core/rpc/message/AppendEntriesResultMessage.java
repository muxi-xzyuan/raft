package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;
import lombok.NonNull;

@Data
public class AppendEntriesResultMessage {

    private final AppendEntriesResult result;
    private final NodeId sourceNodeId;
    @NonNull
    private final AppendEntriesRpc rpc;

}
