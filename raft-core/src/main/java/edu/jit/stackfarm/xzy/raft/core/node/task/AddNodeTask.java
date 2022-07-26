package edu.jit.stackfarm.xzy.raft.core.node.task;


import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;

public class AddNodeTask extends AbstractGroupConfigChangeTask {

    private final NodeEndpoint endpoint;
    private final int nextIndex;
    private final int matchIndex;

    public AddNodeTask(GroupConfigChangeTaskContext context, NodeEndpoint endpoint, NewNodeCatchUpTaskResult newNodeCatchUpTaskResult) {
        this(context, endpoint, newNodeCatchUpTaskResult.getNextIndex(), newNodeCatchUpTaskResult.getMatchIndex());
    }

    public AddNodeTask(GroupConfigChangeTaskContext context, NodeEndpoint endpoint, int nextIndex, int matchIndex) {
        super(context);
        this.endpoint = endpoint;
        this.nextIndex = nextIndex;
        this.matchIndex = matchIndex;
    }

    @Override
    protected void appendGroupConfig() {
        setState(State.GROUP_CONFIG_APPENDED);
        context.addNode(endpoint, nextIndex, matchIndex);
    }

    @Override
    public String toString() {
        return "AddNodeTask{" +
                "state=" + state +
                ", endpoint=" + endpoint +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }

}
