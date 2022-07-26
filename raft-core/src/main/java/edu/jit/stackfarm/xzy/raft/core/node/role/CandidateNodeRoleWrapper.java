package edu.jit.stackfarm.xzy.raft.core.node.role;


import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.schedule.ElectionTimeout;
import jdk.nashorn.internal.ir.annotations.Immutable;
import lombok.Getter;
import lombok.NonNull;

@Getter
@Immutable
public class CandidateNodeRoleWrapper extends AbstractNodeRoleWrapper {
    //获得的票数
    private final int votesCount;
    //选举超时定时器
    private final ElectionTimeout electionTimeout;

    public CandidateNodeRoleWrapper(int term, @NonNull ElectionTimeout electionTimeout) {
        this(term, 1, electionTimeout);
    }

    public CandidateNodeRoleWrapper(int term, int votesCount, @NonNull ElectionTimeout electionTimeout) {
        super(term, NodeRole.CANDIDATE);
        this.votesCount = votesCount;
        this.electionTimeout = electionTimeout;
    }

    @Override
    public NodeId getLeaderId(NodeId selfId) {
        return null;
    }

    @Override
    public void cancelTimeoutOrTask() {
        electionTimeout.cancel();
    }

    @Override
    public String toString() {
        return "Candidate{" +
                "term=" + term +
                ", voteCount=" + votesCount +
                ", electionTimeout=" + electionTimeout +
                '}';
    }
}
