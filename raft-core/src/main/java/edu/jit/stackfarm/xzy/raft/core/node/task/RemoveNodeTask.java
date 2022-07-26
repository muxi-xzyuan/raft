package edu.jit.stackfarm.xzy.raft.core.node.task;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.ToString;

@ToString
public class RemoveNodeTask extends AbstractGroupConfigChangeTask {

    private final NodeId nodeToRemove;

    public RemoveNodeTask(GroupConfigChangeTaskContext context, NodeId nodeToRemove) {
        super(context);
        this.nodeToRemove = nodeToRemove;
    }

    @Override
    protected void appendGroupConfig() {
        setState(State.GROUP_CONFIG_APPENDED);
        context.removeNode(nodeToRemove);
    }

    @Override
    public synchronized void onLogCommitted() {
        if (state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("Log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        if (nodeToRemove.equals(context.getSelfId())) {
            context.downgradeSelf();
        }
        notify();
    }

}
