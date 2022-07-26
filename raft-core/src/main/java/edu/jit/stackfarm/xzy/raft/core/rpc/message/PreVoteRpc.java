package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import lombok.Data;

@Data
public class PreVoteRpc {
    private int term;
    private int lastLogIndex = 0;
    private int lastLogTerm = 0;
}
