package edu.jit.stackfarm.xzy.raft.core.node;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * 某个Follower的复制进度
 */
@Slf4j
@Getter
@Setter
@ToString
public class ReplicatingState {

    //下一个需要复制的日志条目的索引
    private int nextIndex;
    //已匹配日志的索引
    private int matchIndex;
    /**
     * Whether to replicate when receive append entries result
     */
    private boolean replicating = false;
    // for timeout check
    private long lastReplicatedAt = 0;

    ReplicatingState(int nextIndex) {
        this(nextIndex, 0);
    }

    ReplicatingState(int nextIndex, int matchIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    /**
     * Advance next index and match index by last entry index.
     *
     * @param lastEntryIndex last entry index
     * @return true if advanced, false if no change
     */
    boolean advanceNextIndexAndMatchIndex(int lastEntryIndex) {
        // changed
        boolean advanced = (matchIndex != lastEntryIndex || nextIndex != (lastEntryIndex + 1));
        matchIndex = lastEntryIndex;
        nextIndex = lastEntryIndex + 1;
        return advanced;
    }

    /**
     * Back off next index, in other word, decrease.
     *
     * @return true if decrease successfully, false if next index is less than or equal to {@code 1}
     */
    boolean backOffMatchIndex() {
        if (nextIndex > 1) {
            nextIndex--;
            return true;
        }
        return false;
    }
}
