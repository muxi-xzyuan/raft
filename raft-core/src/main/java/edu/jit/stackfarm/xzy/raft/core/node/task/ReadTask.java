package edu.jit.stackfarm.xzy.raft.core.node.task;

import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachine;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesResultMessage;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Record the commit index when receive get command, and notify the statemachine when
 */
@Slf4j
public class ReadTask {

    // commit index when receive get command
    private final int commitIndex;
    // timestamp when receive get command
    private final long timestamp;
    @NonNull
    private final Map<NodeId, Boolean> followerIds;
    @NonNull
    private final StateMachine stateMachine;
    @Getter
    @NonNull
    private final String requestId;
    private int numOfNodesAcknowledgeLeadership;

    public ReadTask(int commitIndex, @NonNull Set<NodeId> followerIds, @NonNull StateMachine stateMachine, @NonNull String requestId) {
        this.commitIndex = commitIndex;
        this.timestamp = System.currentTimeMillis();
        this.followerIds = followerIds.stream()
                .collect(Collectors.toMap((nodeId) -> nodeId, (nodeId) -> false));
        this.stateMachine = stateMachine;
        this.requestId = requestId;
    }

    // node thread

    /**
     * @return true if majority of nodes replied to this node;
     * false if not.
     */
    public boolean receiveSuccessAppendEntriesResult(@NonNull AppendEntriesResultMessage resultMessage) {
        if (followerIds.get(resultMessage.getSourceNodeId())) {
            // true, this follower has acknowledged leadership
            // but we need more nodes to acknowledge, so return false
            return false;
        }
        if (resultMessage.getRpc().getTimestamp() >= timestamp) {
            numOfNodesAcknowledgeLeadership++;
        }
        if (numOfNodesAcknowledgeLeadership + 1 > (followerIds.size() + 1) / 2) {
            stateMachine.leadershipAcknowledged(requestId, commitIndex);
            return true;
        }
        return false;
    }

    public void nodeAdded(NodeId nodeId) {
        followerIds.put(nodeId, Boolean.FALSE);
    }

    public void nodeRemoved(NodeId nodeId) {
        if (followerIds.remove(nodeId)) {
            // true, this follower has acknowledged leadership
            // so we need to reduce the num of nodes that has acknowledged leadership when this follower is removed
            numOfNodesAcknowledgeLeadership--;
        }
    }
}
