package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryMetaData;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;

import java.util.List;
import java.util.Set;

/**
 * 日志条目序列，snapshot保存前部分日志，entry sequence保存后部分日志，snapshot + entry sequence = 所有日志
 */
public interface EntrySequence {

    boolean isEmpty();

    //获取第一条日志的索引
    int getFirstLogIndex();

    //获取最后一条日志的索引
    int getLastLogIndex();

    //获取下一条日志的索引
    int getNextLogIndex();

    List<Entry> subView(int fromIndex);

    /**
     * 获取序列的子视图，从fromIndex(包括)直到最后一条日志(包括)
     */
    List<Entry> subList(int fromIndex);

    /**
     * 获取序列的子视图，从fromIndex(包括)直到toIndex(不包括)
     */
    List<Entry> subList(int fromIndex, int toIndex);

    /**
     * 构建集群配置日志条目历史列表
     *
     * @param initialGroup
     * @return 集群配置日志条目历史列表
     */
    GroupConfigEntryList buildGroupConfigEntryList(Set<NodeEndpoint> initialGroup);

    //檢查某个日志条目是否存在
    boolean isEntryPresent(int index);

    //获取某个日志条目的元信息
    EntryMetaData getEntryMetaData(int index);

    /**
     * Returns the entry whose index is the specified index.
     *
     * @param index index of the entry to return
     * @return the entry whose index is the specified index
     */
    Entry getEntry(int index);

    /**
     * Returns last entry.
     *
     * @return last entry
     */
    Entry getLastEntry();


    /**
     * Append an entry.
     *
     * @param entry entry to be appended
     */
    void append(Entry entry);

    /**
     * Append multiple entries.
     *
     * @param entries entries to be appended
     */
    void append(List<Entry> entries);

    /**
     * Advance commit index to the specified index.
     *
     * @param index index that commit index will be advanced to
     */
    void commit(int index);

    /**
     * Returns current commit index.
     *
     * @return current commit index
     */
    int getCommitIndex();

    /**
     * Remove all the entries after the given {@code index}.
     *
     * @param index high endpoint (inclusive) of the truncated entry sequence
     */
    void removeAfter(int index);

    void close();
}
