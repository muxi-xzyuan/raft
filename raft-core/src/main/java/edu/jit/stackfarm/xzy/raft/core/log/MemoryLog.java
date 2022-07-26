package edu.jit.stackfarm.xzy.raft.core.log;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.EntrySequence;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.GroupConfigEntryList;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.MemoryEntrySequence;
import edu.jit.stackfarm.xzy.raft.core.log.event.SnapshotGeneratedEvent;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.*;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachineContext;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Slf4j
@NotThreadSafe
public class MemoryLog extends AbstractLog {

    private final StateMachineContextImpl stateMachineContext = new StateMachineContextImpl();

    // for test
    public MemoryLog() {
        this(new EventBus(), Collections.emptySet());
    }

    public MemoryLog(EventBus eventBus, Set<NodeEndpoint> initialGroup) {
        this(new EmptySnapshot(), new MemoryEntrySequence(), eventBus, initialGroup);
    }

    public MemoryLog(Snapshot snapshot, EntrySequence entrySequence, EventBus eventBus, Set<NodeEndpoint> initialGroup) {
        super(eventBus);
        setStateMachineContext(stateMachineContext);
        this.snapshot = snapshot;
        this.entrySequence = entrySequence;
        Set<NodeEndpoint> lastGroup = snapshot.getLastConfig();
        this.groupConfigEntryList = new GroupConfigEntryList((lastGroup.isEmpty() ? initialGroup : lastGroup));
    }

    @Override
    protected SnapshotBuilder<MemorySnapshot> newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new MemorySnapshotBuilder(firstRpc);
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        int logIndexOffset = newSnapshot.getLastIncludedIndex() + 1;
        EntrySequence newEntrySequence = new MemoryEntrySequence(logIndexOffset);
        List<Entry> remainingEntries = entrySequence.subView(logIndexOffset);
        newEntrySequence.append(remainingEntries);
        log.debug("Snapshot -> {}", newSnapshot);
        snapshot = newSnapshot;
        log.debug("Entry sequence -> {}", newEntrySequence);
        entrySequence = newEntrySequence;
    }

    @Override
    public void snapshotGenerated(int lastIncludedIndex) {
        if (lastIncludedIndex <= snapshot.getLastIncludedIndex()) {
            return;
        }
        replaceSnapshot(stateMachineContext.buildSnapshot());
    }

    private class StateMachineContextImpl implements StateMachineContext {

        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private int lastIncludedIndex;
        private int lastIncludedTerm;
        private Set<NodeEndpoint> groupConfig;

        @Override
        public OutputStream getOutputForGeneratingSnapshot(int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> groupConfig) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.groupConfig = groupConfig;
            output.reset();
            return output;
        }

        @Override
        public void doneGenerateSnapshot(int lastIncludedIndex) {
            eventBus.post(new SnapshotGeneratedEvent(lastIncludedIndex));
        }

        MemorySnapshot buildSnapshot() {
            return new MemorySnapshot(lastIncludedIndex, lastIncludedTerm, output.toByteArray(), groupConfig);
        }
    }
}
