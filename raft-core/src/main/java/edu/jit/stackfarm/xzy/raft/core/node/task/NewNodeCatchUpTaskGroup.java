package edu.jit.stackfarm.xzy.raft.core.node.task;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesResultMessage;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotResultMessage;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Group for {@link NewNodeCatchUpTask}.
 */
@ThreadSafe
public class NewNodeCatchUpTaskGroup {

    /**
     * 在/待执行任务列表
     */
    private final ConcurrentMap<NodeId, NewNodeCatchUpTask> taskMap = new ConcurrentHashMap<>();

    /**
     * Add task.
     *
     * @param task task
     * @return true if successfully, false if task for same node exists
     */
    public boolean add(NewNodeCatchUpTask task) {
        return taskMap.putIfAbsent(task.getNodeId(), task) == null;
    }

    /**
     * Invoke <code>onReceiveAppendEntriesResult</code> on task.
     *
     * @param resultMessage result message
     * @param nextLogIndex  next index of log
     * @return true if invoked, false if no task for node
     */
    public boolean onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        NewNodeCatchUpTask task = taskMap.get(resultMessage.getSourceNodeId());
        if (task == null) {
            return false;
        }
        task.onReceiveAppendEntriesResult(resultMessage, nextLogIndex);
        return true;
    }

    public boolean onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage, int nextLogIndex) {
        NewNodeCatchUpTask task = taskMap.get(resultMessage.getSourceNodeId());
        if (task == null) {
            return false;
        }
        task.onReceiveInstallSnapshotResult(resultMessage, nextLogIndex);
        return true;
    }

    /**
     * Remove task.
     *
     * @param task task
     * @return {@code true} if removed, {@code false} if not found
     */
    public boolean remove(NewNodeCatchUpTask task) {
        return taskMap.remove(task.getNodeId()) != null;
    }

}
