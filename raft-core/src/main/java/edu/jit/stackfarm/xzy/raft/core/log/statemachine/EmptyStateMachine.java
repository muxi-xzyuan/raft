package edu.jit.stackfarm.xzy.raft.core.log.statemachine;


import edu.jit.stackfarm.xzy.raft.core.log.snapshot.Snapshot;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;

import javax.annotation.Nonnull;
import java.io.OutputStream;
import java.util.Set;

public class EmptyStateMachine implements StateMachine {

    private int lastApplied = 0;

    @Override
    public int getLastApplied() {
        return lastApplied;
    }

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        lastApplied = index;
    }

    @Override
    public void advanceLastApplied(int index) {

    }

    @Override
    public void leadershipAcknowledged(String requestId, int readIndex) {

    }

    @Override
    public boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied) {
        return false;
    }

    @Override
    public void generateSnapshot(@Nonnull OutputStream output) {
    }

    @Override
    public void applySnapshot(@Nonnull Snapshot snapshot) {
        lastApplied = snapshot.getLastIncludedIndex();
    }

    @Override
    public void shutdown() {
    }

}
