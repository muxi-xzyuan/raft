package edu.jit.stackfarm.xzy.raft.core.node.task;

import edu.jit.stackfarm.xzy.raft.core.node.NodeConfig;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesResultMessage;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotResultMessage;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * 多个此种任务可被且应被并发执行
 */
@ToString
public class NewNodeCatchUpTask implements Callable<NewNodeCatchUpTaskResult> {

    private static final Logger logger = LoggerFactory.getLogger(NewNodeCatchUpTask.class);
    @ToString.Exclude
    private final NewNodeCatchUpTaskContext context;
    @ToString.Exclude
    private final NodeConfig config;
    private State state = State.START;
    private final NodeEndpoint nodeEndpoint;
    private boolean done = false;
    // for timeout check
    private long lastReplicateAt;
    // for timeout check
    private long lastAdvanceAt;
    private int nextIndex;
    private int matchIndex;
    private int round = 1;

    public NewNodeCatchUpTask(NewNodeCatchUpTaskContext context, NodeEndpoint nodeEndpoint, NodeConfig config) {
        this.context = context;
        this.nodeEndpoint = nodeEndpoint;
        this.config = config;
    }

    @Override
    public synchronized NewNodeCatchUpTaskResult call() throws Exception {
        logger.debug("New node catch-up task start");
        setState(State.START);
        context.replicateLog(nodeEndpoint);
        lastReplicateAt = System.currentTimeMillis();
        lastAdvanceAt = lastReplicateAt;
        setState(State.REPLICATING);
        while (!done) {
            wait(config.getNewNodeReadTimeout());
            // 1. done
            // 2. replicate -> no response within timeout
            if (System.currentTimeMillis() - lastReplicateAt >= config.getNewNodeReadTimeout()) {
                logger.debug("Node {} not response within read timeout", nodeEndpoint.getId());
                state = State.TIMEOUT;
                break;
            }
        }
        logger.debug("New node catch-up task done");
        context.done(this);
        return mapResult(state);
    }

    public NodeId getNodeId() {
        return nodeEndpoint.getId();
    }

    // in node thread
    synchronized void onReceiveAppendEntriesResult(AppendEntriesResultMessage resultMessage, int nextLogIndex) {
        if (state != State.REPLICATING) {
            throw new IllegalStateException("Receive append entries result when state is not replicating");
        }
        // initialize nextIndex
        if (nextIndex == 0) {
            nextIndex = nextLogIndex;
        }
        logger.debug("Replication state of new node {}, next index {}, match index {}", nodeEndpoint, nextIndex, matchIndex);
        if (resultMessage.getResult().isSuccess()) {
            int lastEntryIndex = resultMessage.getRpc().getLastEntryIndex();
            assert lastEntryIndex >= 0;
            matchIndex = lastEntryIndex;
            nextIndex = lastEntryIndex + 1;
            lastAdvanceAt = System.currentTimeMillis();
            if (nextIndex >= nextLogIndex) {
                setStateAndNotify(State.REPLICATION_CAUGHT_UP);
                return;
            }
            if ((++round) > config.getNewNodeMaxRound()) {
                logger.info("Node {} cannot catch up within max round", nodeEndpoint);
                setStateAndNotify(State.TIMEOUT);
                return;
            }
        } else {
            if (nextIndex <= 1) {
                logger.warn("Node {} cannot back off next index more, stop replication", nodeEndpoint);
                setStateAndNotify(State.REPLICATION_FAILED);
                return;
            }
            nextIndex--;
            if (System.currentTimeMillis() - lastAdvanceAt >= config.getNewNodeAdvanceTimeout()) {
                logger.debug("Node {} cannot make progress within timeout", nodeEndpoint);
                setStateAndNotify(State.TIMEOUT);
                return;
            }
        }
        context.doReplicateLog(nodeEndpoint, nextIndex);
        lastReplicateAt = System.currentTimeMillis();
        notify();
    }

    // in node thread
    synchronized void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage, int nextLogIndex) {
        if (state != State.REPLICATING) {
            throw new IllegalStateException("Receive append entries result when state is not replicating");
        }
        InstallSnapshotRpc rpc = resultMessage.getRpc();
        if (rpc.isDone()) {
            matchIndex = rpc.getLastIndex();
            nextIndex = rpc.getLastIndex() + 1;
            lastAdvanceAt = System.currentTimeMillis();
            if (nextIndex >= nextLogIndex) {
                setStateAndNotify(State.REPLICATION_CAUGHT_UP);
                return;
            }
            round++;
            context.doReplicateLog(nodeEndpoint, nextIndex);
        } else {
            context.sendInstallSnapshot(nodeEndpoint, rpc.getOffset() + rpc.getDataLength());
        }
        lastReplicateAt = System.currentTimeMillis();
        notify();
    }

    private NewNodeCatchUpTaskResult mapResult(State state) {
        switch (state) {
            case REPLICATION_CAUGHT_UP:
                return new NewNodeCatchUpTaskResult(nextIndex, matchIndex);
            case REPLICATION_FAILED:
                return new NewNodeCatchUpTaskResult(NewNodeCatchUpTaskResult.State.REPLICATION_FAILED);
            default:
                return new NewNodeCatchUpTaskResult(NewNodeCatchUpTaskResult.State.TIMEOUT);
        }
    }

    private void setState(State state) {
        logger.debug("State -> {}", state);
        this.state = state;
    }

    private void setStateAndNotify(State state) {
        setState(state);
        done = true;
        notify();
    }

    private enum State {
        START,
        REPLICATING,
        REPLICATION_FAILED,
        REPLICATION_CAUGHT_UP,
        TIMEOUT
    }

}
