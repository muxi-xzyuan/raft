package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import lombok.Data;

import javax.annotation.concurrent.Immutable;

/**
 * 请求投票的结果
 */
@Data
@Immutable
public class RequestVoteResult {
    //选举term
    private final int term;
    //是否投票
    private final boolean voteGranted;

}
