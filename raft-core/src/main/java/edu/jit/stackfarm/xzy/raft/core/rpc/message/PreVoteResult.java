package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PreVoteResult {
    private int term;
    private boolean voteGranted;
}
