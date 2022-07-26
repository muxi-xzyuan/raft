package edu.jit.stackfarm.xzy.raft.core.node.role;


import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.schedule.ElectionTimeout;
import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Getter
@Immutable
public class FollowerNodeRoleWrapper extends AbstractNodeRoleWrapper {

    //当前leader节点ID，可能为空
    private final NodeId leaderId;
    //投过票的节点，可能为空
    private final NodeId votedFor;

    private final int preVotesCount;
    //选举超时定时器
    private final ElectionTimeout electionTimeout;

    public FollowerNodeRoleWrapper(int term, @Nullable NodeId votedFor, @Nullable NodeId leaderId, int preVotesCount, @NonNull ElectionTimeout electionTimeout) {
        super(term, NodeRole.FOLLOWER);
        this.votedFor = votedFor;
        this.leaderId = leaderId;
        this.preVotesCount = preVotesCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return leaderId;
    }

    @Override
    public void cancelTimeoutOrTask() {
        electionTimeout.cancel();
    }

    @Override
    public String toString() {
        return "Follower{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", votedFor=" + votedFor +
                ", electionTimeout=" + electionTimeout +
                '}';
    }
}
