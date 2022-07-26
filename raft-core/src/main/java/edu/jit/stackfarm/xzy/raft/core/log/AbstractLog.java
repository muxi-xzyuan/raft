package edu.jit.stackfarm.xzy.raft.core.log;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.log.entry.*;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.EntrySequence;
import edu.jit.stackfarm.xzy.raft.core.log.entry.sequence.GroupConfigEntryList;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.*;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.EmptyStateMachine;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachine;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachineContext;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesRpc;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Slf4j
abstract class AbstractLog implements Log {

    protected final EventBus eventBus;
    protected EntrySequence entrySequence;
    @Setter
    @Getter
    protected StateMachine stateMachine = new EmptyStateMachine();
    protected GroupConfigEntryList groupConfigEntryList;
    protected Snapshot snapshot;
    protected SnapshotBuilder<?> snapshotBuilder = new DumbSnapshotBuilder();
    private StateMachineContext stateMachineContext;

    AbstractLog(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public int getLastLogTerm() {
        if (entrySequence.isEmpty()) {
            return snapshot.getLastIncludedTerm();
        }
        return entrySequence.getLastEntry().getTerm();
    }

    @Override
    public int getLastLogIndex() {
        if (entrySequence.isEmpty()) {
            return snapshot.getLastIncludedIndex();
        }
        return entrySequence.getLastEntry().getIndex();
    }

    @Override
    public AppendEntriesRpc createAppendEntriesRpc(int term, NodeId selfId, int nextIndex, int maxEntries) throws EntryInSnapshotException {
        //检查nextIndex
        int nextLogIndex = entrySequence.getNextLogIndex();
        if (nextIndex > nextLogIndex) {
            throw new IllegalArgumentException("Illegal next index " + nextIndex);
        }
        //日志在快照中
        if (nextIndex <= snapshot.getLastIncludedIndex()) {
            throw new EntryInSnapshotException(nextIndex);
        }
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setMessageId(UUID.randomUUID().toString());
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLeaderCommit(getCommitIndex());
        rpc.setTimestamp(System.currentTimeMillis());

        if (nextIndex == snapshot.getLastIncludedIndex() + 1) {
            // entry sequence is empty
            rpc.setPrevLogIndex(snapshot.getLastIncludedIndex());
            rpc.setPrevLogTerm(snapshot.getLastIncludedTerm());
        } else {
            // if entry sequence is empty,
            // snapshot.lastIncludedIndex + 1 == nextLogIndex,
            // so it has been rejected at the first line.
            //
            // if entry sequence is not empty,
            // snapshot.lastIncludedIndex + 1 < nextIndex <= nextLogIndex
            // and snapshot.lastIncludedIndex + 1 = firstLogIndex
            //     nextLogIndex = lastLogIndex + 1
            // then firstLogIndex < nextIndex <= lastLogIndex + 1
            //      firstLogIndex + 1 <= nextIndex <= lastLogIndex + 1
            //      firstLogIndex <= nextIndex - 1 <= lastLogIndex
            // it is ok to get entry without null check
            Entry entry = entrySequence.getEntry(nextIndex - 1);
            assert entry != null;
            rpc.setPrevLogIndex(entry.getIndex());
            rpc.setPrevLogTerm(entry.getTerm());
        }
        if (!entrySequence.isEmpty()) {
            int maxIndex = (maxEntries == ALL_ENTRIES ? nextLogIndex : Math.min(nextLogIndex, nextIndex + maxEntries));
            rpc.setEntries(entrySequence.subList(nextIndex, maxIndex));
        }
        return rpc;
    }

    @Override
    public InstallSnapshotRpc createInstallSnapshotRpc(int term, NodeId selfId, int offset, int length) {
        InstallSnapshotRpc rpc = new InstallSnapshotRpc();
        rpc.setTerm(term);
        rpc.setLeaderId(selfId);
        rpc.setLastIndex(snapshot.getLastIncludedIndex());
        rpc.setTerm(snapshot.getLastIncludedTerm());
        if (offset == 0) {
            rpc.setLastConfig(snapshot.getLastConfig());
        }
        rpc.setOffset(offset);
        SnapshotChunk chunk = snapshot.readData(offset, length);
        rpc.setData(chunk.toByteArray());
        rpc.setDone(chunk.isLastChunk());
        return rpc;
    }

    @Override
    public int getNextIndex() {
        return entrySequence.getNextLogIndex();
    }

    @Override
    public int getCommitIndex() {
        return entrySequence.getCommitIndex();
    }

    /**
     * @return true 如果本地的index和term均比对方的大
     */
    @Override
    public boolean isNewerThan(int lastLogIndex, int lastLogTerm) {
        return getLastLogTerm() > lastLogTerm || getLastLogIndex() > lastLogIndex;
    }

    @Override
    public NoOpEntry appendEntry(int term) {
        NoOpEntry entry = new NoOpEntry(entrySequence.getNextLogIndex(), term);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public GeneralEntry appendEntry(int term, byte[] command) {
        GeneralEntry entry = new GeneralEntry(entrySequence.getNextLogIndex(), term, command);
        entrySequence.append(entry);
        return entry;
    }

    @Override
    public AddNodeEntry appendEntryForAddNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeEndpoint newNodeEndpoint) {
        AddNodeEntry entry = new AddNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, newNodeEndpoint);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public RemoveNodeEntry appendEntryForRemoveNode(int term, Set<NodeEndpoint> nodeEndpoints, NodeId nodeToRemove) {
        RemoveNodeEntry entry = new RemoveNodeEntry(entrySequence.getNextLogIndex(), term, nodeEndpoints, nodeToRemove);
        entrySequence.append(entry);
        groupConfigEntryList.add(entry);
        return entry;
    }

    @Override
    public AppendEntriesState appendEntriesFromLeader(int prevLogIndex, int prevLogTerm, List<Entry> entries) {
        //检查前一条日志是否匹配
        if (!checkIfPreviousLogMatches(prevLogIndex, prevLogTerm)) {
            return AppendEntriesState.FAILED;
        }
        if (entries.isEmpty()) {
            //heartbeat
            return AppendEntriesState.SUCCESS;
        }
        UnmatchedLogRemovedResult unmatchedLogRemovedResult = removeUnmatchedLog(new EntrySequenceView(entries));
        GroupConfigEntry appendedGroupConfigEntry = appendEntriesFromLeader(unmatchedLogRemovedResult.newEntries);
        Set<NodeEndpoint> latestGroup = appendedGroupConfigEntry != null ?
                appendedGroupConfigEntry.getResultNodeEndpoints() :
                unmatchedLogRemovedResult.getGroupConfigBeforeRemovedGroupConfigEntryAppended();
        return new AppendEntriesState(latestGroup);
    }

    @Override
    public List<GroupConfigEntry> advanceCommitIndex(int newCommitIndex, int currentTerm) {
        if (!validNewCommitIndex(newCommitIndex, currentTerm)) {
            return Collections.emptyList();
        }
        int oldCommitIndex = entrySequence.getCommitIndex();
        log.info("Advance commit index from {} to {}, committed entries {}",
                oldCommitIndex, newCommitIndex, entrySequence.subList(oldCommitIndex + 1, newCommitIndex + 1));
        entrySequence.commit(newCommitIndex);
        List<GroupConfigEntry> groupConfigEntries = groupConfigEntryList.subList(oldCommitIndex + 1, newCommitIndex + 1);
        advanceApplyIndex();
        return groupConfigEntries;
    }

    @Override
    public InstallSnapshotState installSnapshot(InstallSnapshotRpc rpc) {
        //如果消息中的lastIncludedIndex比当前日志快照的小，则忽略
        if (rpc.getLastIndex() <= snapshot.getLastIncludedIndex()) {
            log.debug("Snapshot's last included index from rpc <= current one({} <= {}), ignore",
                    rpc.getLastIndex(), snapshot.getLastIncludedIndex());
            return new InstallSnapshotState(InstallSnapshotState.StateName.ILLEGAL_INSTALL_SNAPSHOT_RPC);
        }
        //如果偏移为0，则重置日志快照（关闭加创建）
        if (rpc.getOffset() == 0) {
            snapshotBuilder.close();
            snapshotBuilder = newSnapshotBuilder(rpc);
        } else {
            //否则追加，builder会判断连续多个消息中的lastIncludedIndex等是否一致
            snapshotBuilder.append(rpc);
        }
        //尚未结束
        if (!rpc.isDone()) {
            return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLING);
        }
        //准备完成
        Snapshot newSnapshot = snapshotBuilder.build();
        //更新日志
        replaceSnapshot(newSnapshot);
        //应用日志
        applySnapshot(newSnapshot);
        return new InstallSnapshotState(InstallSnapshotState.StateName.INSTALLED, newSnapshot.getLastConfig());
    }

    @Nonnull
    @Override
    @SuppressWarnings("null")
    public Set<NodeEndpoint> getLatestGroupConfig() {
        if (!groupConfigEntryList.isEmpty()) {
            return snapshot.getLastConfig();
        }
        GroupConfigEntry lastGroupConfigEntry = groupConfigEntryList.getLast();
        if (lastGroupConfigEntry != null) {
            return lastGroupConfigEntry.getResultNodeEndpoints();
        }
        return Collections.emptySet();
    }

    @Override
    public void close() {
        snapshot.close();
        entrySequence.close();
        snapshotBuilder.close();
        stateMachine.shutdown();
    }

    void setStateMachineContext(StateMachineContext stateMachineContext) {
        this.stateMachineContext = stateMachineContext;
    }

    private void applySnapshot(Snapshot snapshot) {
        log.debug("Apply snapshot, last included index {}", snapshot.getLastIncludedIndex());
        try {
            stateMachine.applySnapshot(snapshot);
        } catch (IOException e) {
            throw new LogException("Failed to apply snapshot", e);
        }
    }

    private boolean checkIfPreviousLogMatches(int prevLogIndex, int prevLogTerm) {
        int lastIncludedIndex = snapshot.getLastIncludedIndex();
        if (prevLogIndex < lastIncludedIndex) {
            //前一条日志的索引比日志快照要小
            log.debug("Previous log index {} < snapshot's last included index {}", prevLogIndex, lastIncludedIndex);
            return false;
        }
        if (prevLogIndex == lastIncludedIndex) {
            int lastIncludedTerm = snapshot.getLastIncludedTerm();
            if (prevLogTerm != lastIncludedTerm) {
                //索引匹配，term不匹配
                log.debug("Previous log index matches snapshot's last included index, " +
                        "but term not (expected {}, actual {})", lastIncludedTerm, prevLogTerm);
                return false;
            }
            return true;
        }
        EntryMetaData metaData = entrySequence.getEntryMetaData(prevLogIndex);
        if (metaData == null) {
            log.debug("Previous log {} not found", prevLogIndex);
            return false;
        }
        int term = metaData.getTerm();
        if (term != prevLogTerm) {
            log.debug("Different term of previous log, local {}, remote {}", term, prevLogTerm);
            return false;
        }
        return true;
    }

    private UnmatchedLogRemovedResult removeUnmatchedLog(EntrySequenceView leaderEntries) {
        int firstUnmatched = findFirstUnmatchedLog(leaderEntries);
        if (firstUnmatched < 0) {
            //If firstUnmatched < 0, the entries from leader are all exists in this follower's log.
            //So there is no need to append any entry.
            //Happen when certain network delay happens.
            return new UnmatchedLogRemovedResult(EntrySequenceView.EMPTY, null);
        }
        return new UnmatchedLogRemovedResult(leaderEntries.subView(firstUnmatched), removeEntriesAfter(firstUnmatched - 1));
    }

    /**
     * @param leaderEntries 来自leader的日志条目
     * @return the index of the first entry in the leader's entries that does not match the follower's entries;
     * -1 if all match.
     */
    private int findFirstUnmatchedLog(EntrySequenceView leaderEntries) {
        for (Entry leaderEntry : leaderEntries) {
            int logIndex = leaderEntry.getIndex();
            EntryMetaData followerEntryMetaData = entrySequence.getEntryMetaData(logIndex);
            if (followerEntryMetaData == null || followerEntryMetaData.getTerm() != leaderEntry.getTerm()) {
                return logIndex;
            }
        }
        return -1;
    }

    /**
     * Remove entries whose index is greater than the given index.
     *
     * @return the GroupConfigEntry removed or null if no GroupConfigEntry is removed.
     * @throws IllegalArgumentException for nothing to remove or trying to remove applied entry
     */
    @Nullable
    private GroupConfigEntry removeEntriesAfter(int index) {
        if (entrySequence.isEmpty() || index >= entrySequence.getLastLogIndex()) {
            return null;
        }
        int lastApplied = stateMachine.getLastApplied();
        if (index < lastApplied) {
            // should not happen
            throw new IllegalArgumentException("Trying to remove applied entry! Index " + index + " last applied " + lastApplied);
        }
        log.debug("Remove entries after {}", index);
        entrySequence.removeAfter(index);
        return groupConfigEntryList.removeAfter(index);
    }

    /**
     * @param leaderEntries the entries from leader.
     * @return the appended group config entry or null if no group config entry is in the leaderEntries.
     */
    @Nullable
    private GroupConfigEntry appendEntriesFromLeader(EntrySequenceView leaderEntries) {
        if (leaderEntries.isEmpty()) {
            return null;
        }
        log.info("Append entries {} from leader", leaderEntries);
        GroupConfigEntry groupConfigEntry = null;
        for (Entry leaderEntry : leaderEntries) {
            entrySequence.append(leaderEntry);
            if (leaderEntry instanceof GroupConfigEntry) {
                groupConfigEntry = (GroupConfigEntry) leaderEntry;
            }
        }
        return groupConfigEntry;
    }

    //检查新的commitIndex
    private boolean validNewCommitIndex(int newCommitIndex, int currentTerm) {
        log.debug("Validating new commit index, current commit index {}, new commit index {}", entrySequence.getCommitIndex(), newCommitIndex);
        //小于当前的commitIndex
        if (newCommitIndex <= entrySequence.getCommitIndex()) {
            return false;
        }
        EntryMetaData metaData = entrySequence.getEntryMetaData(newCommitIndex);
        if (metaData == null) {
            return false;
        }
        //日志条目的term必须是当前term，才可推进commitIndex
        return metaData.getTerm() == currentTerm;
    }

    /**
     * Apply all committed entry.
     */
    private void advanceApplyIndex() {
        // start up and snapshot exists
        if (stateMachine.getLastApplied() == 0 && snapshot.getLastIncludedIndex() > 0) {
            applySnapshot(snapshot);
        }
        for (Entry entry : entrySequence.subList(stateMachine.getLastApplied() + 1, getCommitIndex() + 1)) {
            applyEntry(entry);
        }
    }

    private void applyEntry(Entry entry) {
        if (isApplicable(entry)) {
            Set<NodeEndpoint> lastGroupConfig = groupConfigEntryList.getLastGroupBefore(entry.getIndex());
            stateMachine.applyLog(stateMachineContext, entry.getIndex(), entry.getTerm(), entry.getCommandBytes(), entrySequence.getFirstLogIndex(), lastGroupConfig);
        } else {
            // for no-op entry and membership-config entry, advance last applied directly.
            stateMachine.advanceLastApplied(entry.getIndex());
        }
    }

    private boolean isApplicable(Entry entry) {
        return entry.getKind() == Entry.KIND_GENERAL;
    }

    protected abstract SnapshotBuilder<?> newSnapshotBuilder(InstallSnapshotRpc firstRpc);

    protected abstract void replaceSnapshot(Snapshot newSnapshot);

    private static class EntrySequenceView implements Iterable<Entry> {
        static final EntrySequenceView EMPTY = new EntrySequenceView(Collections.emptyList());
        private final List<Entry> entries;
        @Getter
        private int firstLogIndex = -1;
        @Getter
        private int lastLogIndex = -1;

        EntrySequenceView(List<Entry> entries) {
            this.entries = entries;
            if (!entries.isEmpty()) {
                firstLogIndex = entries.get(0).getIndex();
                lastLogIndex = entries.get(entries.size() - 1).getIndex();
            }
        }

        Entry get(int index) {
            if (entries.isEmpty() || index < firstLogIndex || index > lastLogIndex) {
                return null;
            }
            return entries.get(index - firstLogIndex);
        }

        boolean isEmpty() {
            return entries.isEmpty();
        }

        EntrySequenceView subView(int fromIndex) {
            if (entries.isEmpty() || fromIndex > lastLogIndex) {
                return new EntrySequenceView(Collections.emptyList());
            }
            return new EntrySequenceView(entries.subList(fromIndex - firstLogIndex, entries.size()));
        }

        @Override
        public Iterator<Entry> iterator() {
            return entries.iterator();
        }

        @Override
        public String toString() {
            return entries.toString();
        }
    }

    private static class UnmatchedLogRemovedResult {
        private final EntrySequenceView newEntries;
        private final GroupConfigEntry removedGroupConfigEntry;

        UnmatchedLogRemovedResult(EntrySequenceView newEntries, GroupConfigEntry removedGroupConfigEntry) {
            this.newEntries = newEntries;
            this.removedGroupConfigEntry = removedGroupConfigEntry;
        }

        /**
         * @return the group config before the removed group config entry was appended;
         * or null if no group config entry was removed.
         */
        @Nullable
        Set<NodeEndpoint> getGroupConfigBeforeRemovedGroupConfigEntryAppended() {
            return removedGroupConfigEntry != null ? removedGroupConfigEntry.getNodeEndpoints() : null;
        }
    }
}
