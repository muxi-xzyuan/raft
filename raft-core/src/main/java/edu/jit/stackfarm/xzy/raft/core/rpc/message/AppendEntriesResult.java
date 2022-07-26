package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import lombok.Data;

import javax.annotation.concurrent.Immutable;

/**
 * 追加日志条目的结果
 */
@Data
@Immutable
public class AppendEntriesResult {

    private final int term;
    private final boolean success;
    private final String messageId;

}
