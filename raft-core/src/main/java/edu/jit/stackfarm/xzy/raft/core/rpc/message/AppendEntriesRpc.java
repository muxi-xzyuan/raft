package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * 追加日志条目RPC
 */
@Data
public class AppendEntriesRpc {

    private int term;
    private NodeId leaderId;
    private int leaderCommit;
    private List<Entry> entries = Collections.emptyList();
    private int prevLogIndex;
    private int prevLogTerm;
    private long timestamp;
    private String messageId;

    /**
     * 获得最后一条日志条目的下标
     *
     * @return 最后一条日志条目的下标
     */
    public int getLastEntryIndex() {
        return this.entries.isEmpty() ? this.prevLogIndex : this.entries.get(this.entries.size() - 1).getIndex();
    }
}