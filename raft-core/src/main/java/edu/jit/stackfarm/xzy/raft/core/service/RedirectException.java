package edu.jit.stackfarm.xzy.raft.core.service;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;

public class RedirectException extends ChannelException {

    private final NodeId leaderId;

    public RedirectException(NodeId leaderId) {
        this.leaderId = leaderId;
    }

    public NodeId getLeaderId() {
        return leaderId;
    }

}
