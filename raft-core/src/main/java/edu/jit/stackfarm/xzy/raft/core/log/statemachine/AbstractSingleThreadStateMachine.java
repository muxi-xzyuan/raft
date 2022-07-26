package edu.jit.stackfarm.xzy.raft.core.log.statemachine;

import edu.jit.stackfarm.xzy.raft.core.log.snapshot.Snapshot;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.support.SingleThreadTaskExecutor;
import edu.jit.stackfarm.xzy.raft.core.support.TaskExecutor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Slf4j
public abstract class AbstractSingleThreadStateMachine implements StateMachine {

    private final TaskExecutor taskExecutor;
    @Getter
    private volatile int lastApplied = 0;
    // readIndex, requestIds
    private final SortedMap<Integer, List<String>> readIndexMap = new TreeMap<>();

    public AbstractSingleThreadStateMachine() {
        taskExecutor = new SingleThreadTaskExecutor("state-machine");
    }

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @NonNull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        taskExecutor.execute(() ->
                doApplyLog(context, index, term, commandBytes, firstLogIndex, lastGroupConfig));
    }

    @Override
    public void advanceLastApplied(int index) {
        taskExecutor.execute(() -> {
            if (index <= lastApplied) {
                return;
            }
            lastApplied = index;
            onLastAppliedAdvanced();
        });
    }

    @Override
    public void leadershipAcknowledged(String requestId, int readIndex) {
        taskExecutor.execute(() -> {
            if (lastApplied >= readIndex) {
                onReadIndexReached(requestId);
            } else {
                log.debug("Waiting for last applied index {} to reach read index {}", lastApplied, readIndex);
                List<String> requestIds = readIndexMap.get(readIndex);
                if (requestIds == null) {
                    requestIds = new LinkedList<>();
                }
                requestIds.add(requestId);
                readIndexMap.put(readIndex, requestIds);
            }
        });
    }

    @Override
    public void applySnapshot(Snapshot snapshot) {
        taskExecutor.execute(() -> {
            log.info("apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
            try {
                doApplySnapshot(snapshot.getDataStream());
                lastApplied = snapshot.getLastIncludedIndex();
            } catch (IOException e) {
                log.warn("failed to apply snapshot", e);
            }
        });
    }

    @Override
    public void shutdown() {
        taskExecutor.shutdown();
    }

    private void doApplyLog(StateMachineContext context, int index, int term, @NonNull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        //忽略已应用过的日志
        if (index <= lastApplied) {
            return;
        }
        log.debug("Apply log {}", index);
        applyCommand(commandBytes);
        lastApplied = index;
        onLastAppliedAdvanced();
        if (shouldGenerateSnapshot(firstLogIndex, lastApplied)) {
            try {
                OutputStream output = context.getOutputForGeneratingSnapshot(index, term, lastGroupConfig);
                generateSnapshot(output);
                context.doneGenerateSnapshot(index);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void onLastAppliedAdvanced() {
        log.debug("Last applied index advanced, {}", lastApplied);
        SortedMap<Integer, List<String>> readIndexReachedRequestIds = readIndexMap.headMap(lastApplied + 1);
        for (List<String> requestIds : readIndexReachedRequestIds.values()) {
            for (String requestId : requestIds) {
                onReadIndexReached(requestId);
            }
        }
        readIndexReachedRequestIds.clear();
    }

    protected abstract void applyCommand(byte[] commandBytes);

    protected abstract void onReadIndexReached(String requestId);

    protected abstract void doApplySnapshot(InputStream input) throws IOException;

}
