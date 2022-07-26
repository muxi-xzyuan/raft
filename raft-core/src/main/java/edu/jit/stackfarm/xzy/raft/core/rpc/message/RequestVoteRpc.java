package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;

import java.io.Serializable;

/**
 * 请求投票远程调用，由Candidate发起
 */
@Data
public class RequestVoteRpc implements Serializable {
    //选举term
    private int term;
    //候选者节点ID，一般是发送者自己
    private NodeId candidateId;
    //候选者最后一条日志的索引
    private int lastLogIndex;
    //候选者最后一条日志的term
    private int lastLogTerm;
}
