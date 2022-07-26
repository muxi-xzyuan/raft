package edu.jit.stackfarm.xzy.raft.core.log;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.EntrySequence;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.FileEntrySequence;
import edu.jit.stackfarm.xzy.raft.core.log.event.SnapshotGeneratedEvent;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.*;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachineContext;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

public class FileLog extends AbstractLog {

    private final RootDir rootDir;

    public FileLog(File baseDir, EventBus eventBus, Set<NodeEndpoint> initialGroup) {
        super(eventBus);
        setStateMachineContext(new StateMachineContextImpl());
        rootDir = new RootDir(baseDir);
        LogGeneration latestGeneration = rootDir.getLatestGeneration();
        snapshot = new EmptySnapshot();
        if (latestGeneration != null) {
            if (latestGeneration.getSnapshotFile().exists()) {
                snapshot = new FileSnapshot(latestGeneration);
                initialGroup = snapshot.getLastConfig();
            }
            entrySequence = new FileEntrySequence(latestGeneration, latestGeneration.getLastIncludedIndex());
            groupConfigEntryList = entrySequence.buildGroupConfigEntryList(initialGroup);
        } else {
            LogGeneration firstGeneration = rootDir.createFirstGeneration();
            entrySequence = new FileEntrySequence(firstGeneration, 1);
        }
    }

    @Override
    protected SnapshotBuilder<FileSnapshot> newSnapshotBuilder(InstallSnapshotRpc firstRpc) {
        return new FileSnapshotBuilder(firstRpc, rootDir.getLogDirForInstalling());
    }

    @Override
    protected void replaceSnapshot(Snapshot newSnapshot) {
        FileSnapshot fileSnapshot = (FileSnapshot) newSnapshot;
        int lastIncludedIndex = fileSnapshot.getLastIncludedIndex();
        int logIndexOffset = lastIncludedIndex + 1;
        //取剩下的日志
        List<Entry> remainingEntries = entrySequence.subView(logIndexOffset);
        //写入日志快照所在目录
        EntrySequence newEntrySequence = new FileEntrySequence(fileSnapshot.getLogDir(), logIndexOffset);
        newEntrySequence.append(remainingEntries);
        newEntrySequence.commit(Math.max(getCommitIndex(), lastIncludedIndex));
        newEntrySequence.close();
        //关闭现有日志快照、日志等文件
        snapshot.close();
        entrySequence.close();
        newSnapshot.close();
        //重命名
        LogDir generation = rootDir.rename(fileSnapshot.getLogDir(), lastIncludedIndex);
        snapshot = new FileSnapshot(generation);
        entrySequence = new FileEntrySequence(generation, logIndexOffset);
        groupConfigEntryList = entrySequence.buildGroupConfigEntryList(snapshot.getLastConfig());
    }

    @Override
    public void snapshotGenerated(int lastIncludedIndex) {
        if (lastIncludedIndex <= snapshot.getLastIncludedIndex()) {
            return;
        }
        replaceSnapshot(new FileSnapshot(rootDir.getLogDirForGenerating()));
    }

    private class StateMachineContextImpl implements StateMachineContext {

        private FileSnapshotWriter snapshotWriter = null;

        @Override
        public OutputStream getOutputForGeneratingSnapshot(int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> groupConfig) throws IOException {
            if (snapshotWriter != null) {
                snapshotWriter.close();
            }
            snapshotWriter = new FileSnapshotWriter(rootDir.getLogDirForGenerating().getSnapshotFile(), lastIncludedIndex, lastIncludedTerm, groupConfig);
            return snapshotWriter.getOutput();
        }

        @Override
        public void doneGenerateSnapshot(int lastIncludedIndex) throws IOException {
            if (snapshotWriter == null) {
                throw new IllegalStateException("Snapshot not created");
            }
            snapshotWriter.close();
            eventBus.post(new SnapshotGeneratedEvent(lastIncludedIndex));
        }
    }
}
