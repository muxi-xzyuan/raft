package edu.jit.stackfarm.xzy.raft.core.node.role;


import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.schedule.LogReplicationTask;
import jdk.nashorn.internal.ir.annotations.Immutable;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Immutable
public class LeaderNodeRoleWrapper extends AbstractNodeRoleWrapper {
    //日志复制定时器
    private final LogReplicationTask logReplicationTask;

    public LeaderNodeRoleWrapper(int term, @NonNull LogReplicationTask logReplicationTask) {
        super(term, NodeRole.LEADER);
        this.logReplicationTask = logReplicationTask;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return selfId;
    }

    @Override
    public void cancelTimeoutOrTask() {
        logReplicationTask.cancel();
    }

    @Override
    public String toString() {
        return "Leader{" +
                "term=" + term +
                ", logReplicationTask=" + logReplicationTask +
                '}';
    }
}
