package edu.jit.stackfarm.xzy.raft.core.node;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeAlreadyAddedException extends Exception {

    private final NodeId nodeId;

}
