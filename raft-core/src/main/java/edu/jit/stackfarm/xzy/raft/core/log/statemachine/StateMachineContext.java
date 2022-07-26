package edu.jit.stackfarm.xzy.raft.core.log.statemachine;

import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

public interface StateMachineContext {

    OutputStream getOutputForGeneratingSnapshot(int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> groupConfig) throws IOException;

    void doneGenerateSnapshot(int lastIncludedIndex) throws IOException;

}
