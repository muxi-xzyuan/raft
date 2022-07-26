package edu.jit.stackfarm.xzy.raft.core.node;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class GroupMember {

    @EqualsAndHashCode.Include
    private final NodeEndpoint endpoint;
    private ReplicatingState replicatingState;

    GroupMember(NodeEndpoint endpoint) {
        this(endpoint, null);
    }

    public GroupMember(NodeEndpoint endpoint, ReplicatingState replicatingState) {
        this.endpoint = endpoint;
        this.replicatingState = replicatingState;
    }

    ReplicatingState getReplicatingState() {
        if (replicatingState == null) {
            throw new IllegalStateException("Replication state not set");
        }
        return replicatingState;
    }

    boolean idEquals(NodeId nodeId) {
        return endpoint.getId().equals(nodeId);
    }

    void replicateNow() {
        replicateAt(System.currentTimeMillis());
    }

    void stopReplicating() {
        getReplicatingState().setReplicating(false);
    }

    boolean shouldReplicate(long readTimeout) {
        ReplicatingState replicatingState = getReplicatingState();
        // case1: replicate when receive append entries result, so there is no need to replicate in schedule task
        // case2: it has been too long before last replication, so we need to replicate in this schedule task
        return !replicatingState.isReplicating() || System.currentTimeMillis() - replicatingState.getLastReplicatedAt() >= readTimeout;
    }

    void replicateAt(long replicateAt) {
        ReplicatingState replicatingState = getReplicatingState();
        replicatingState.setReplicating(true);
        replicatingState.setLastReplicatedAt(replicateAt);
    }
}