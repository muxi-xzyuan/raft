package edu.jit.stackfarm.xzy.raft.core.log;

import edu.jit.stackfarm.xzy.raft.core.log.entry.*;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.EntryInSnapshotException;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.InstallSnapshotState;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachine;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesRpc;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * 日志组件核心接口
 */
public interface Log {

    int ALL_ENTRIES = -1;

    int getLastLogTerm();

    int getLastLogIndex();

    //创建AppendEntries消息
    AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfId, int nextIndex, int maxEntries) throws EntryInSnapshotException;

    /**
     * 创建安装日志rpc
     */
    InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfId, int offset, int length);

    //获取下一条日志的索引
    int getNextIndex();

    //获取当前的CommitIndex
    int getCommitIndex();

    /**
     * 判断对方的lastLogIndex和lastLogTerm是否比自己新
     *
     * @param lastLogIndex 对方的lastLogIndex
     * @param lastLogTerm  对方的lastLogTerm
     * @return true 如果对方的lastLogIndex比己方的lastLogIndex新且对方的lastLogTerm比己方的lastLongTerm新；
     * false 如果对方的lastLogIndex不比己方的lastLogIndex新或对方的lastLogIndex不必己方的lastLogTerm新
     */
    boolean isNewerThan(int lastLogIndex, int lastLogTerm);

    //追加一条NO-OP日志
    NoOpEntry appendEntry(int term);

    //追加一条普通日志
    GeneralEntry appendEntry(int term, byte[] command);

    AddNodeEntry appendEntryForAddNode(int term, Set<NodeEndpoint> endpoints, NodeEndpoint newNodeEndpoint);

    RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeEndpoint> endpoints, NodeId nodeToRemove);

    //追加数条日志
    AppendEntriesState appendEntriesFromLeader(int prevLogIndex, int prevLogTerm, List<Entry> entries);

    //推进commitIndex
    List<GroupConfigEntry> advanceCommitIndex(int newCommitIndex, int currentTerm);

    /**
     * 安装日志快照
     *
     * @return 安装状态
     */
    InstallSnapshotState installSnapshot(InstallSnapshotRpc rpc);

    void snapshotGenerated(int lastIncludedIndex);

    /**
     * 获得最新的集群配置
     *
     * @return 最新的集群配置
     */
    @Nonnull
    Set<NodeEndpoint> getLatestGroupConfig();

    /**
     * 注册状态机
     *
     * @param stateMachine 状态机实例
     */
    void setStateMachine(StateMachine stateMachine);

    StateMachine getStateMachine();

    void close();
}
