package edu.jit.stackfarm.xzy.raft.core.node.task;


import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;

/**
 * Task context for {@link GroupConfigChangeTask}.
 */
public interface GroupConfigChangeTaskContext {

    /**
     * Add node.
     *
     * @param endpoint
     * @param nextIndex
     * @param matchIndex
     */
    void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex);

    /**
     * Downgrade self, only called when this node is the leader and the node are removed from the group.
     */
    void downgradeSelf();

    /**
     * Remove node from group.
     *
     * @param nodeId the id of node to be removed
     */
    void removeNode(NodeId nodeId);

    NodeId getSelfId();

    /**
     * Done and remove current group config change task.
     */
    void done();

}
