package edu.jit.stackfarm.xzy.raft.core.log.statemachine;

import edu.jit.stackfarm.xzy.raft.core.log.snapshot.Snapshot;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.NonNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Set;

/**
 * A statemachine is a data container and can apply command to change the data.
 */
public interface StateMachine {

    /**
     * Get the index of the last applied entry.
     *
     * @return the index of the last applied entry.
     */
    int getLastApplied();

    /**
     * Apply log entry.
     *
     * @param context         statemachine context.
     * @param index           the index of log entry that will be applied.
     * @param term            term.
     * @param commandBytes    the bytes of the command.
     * @param firstLogIndex   the index of the first log entry in the entry sequence.
     * @param lastGroupConfig
     */
    void applyLog(StateMachineContext context, int index, int term, @NonNull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig);

    /**
     * Increase the last applied index to a new number specified by the argument index.
     *
     * @param index new last applied index.
     */
    void advanceLastApplied(int index);

    /**
     * When receive get command, need to check whether this node is really a leader.
     *
     * @param requestId the id of get command.
     * @param readIndex the commit index when receive get command.
     */
    void leadershipAcknowledged(String requestId, int readIndex);

    void applySnapshot(Snapshot snapshot) throws IOException;

    /**
     * Whether snapshot should be generated.
     *
     * @param firstLogIndex the index of the first log entry in the entry sequence.
     * @param lastApplied   the index of the last applied entry.
     * @return true if should, false if should not.
     */
    boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied);

    void generateSnapshot(OutputStream output) throws IOException;

    void shutdown();
}
