package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryMetaData;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
abstract class AbstractEntrySequence implements EntrySequence {

    //日志条目序列的起始条目在所有日志（snapshot+entry sequence）中的位置
    //最小为1，如果为1则表示当前不存在snapshot
    protected int logIndexOffset;
    //nextLogIndex = 最后的日志条目的下标 +1
    //如果logIndexOffset == nextLogIndex，则表示当前entry sequence为空
    protected int nextLogIndex;
    @Getter
    protected int commitIndex;

    public AbstractEntrySequence(int logIndexOffset) {
        this.logIndexOffset = logIndexOffset;
        this.nextLogIndex = logIndexOffset;
    }

    @Override
    public boolean isEmpty() {
        return logIndexOffset == nextLogIndex;
    }

    @Override
    public int getFirstLogIndex() {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        return doGetFirstLogIndex();
    }

    @Override
    public int getLastLogIndex() {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        return doGetLastLogIndex();
    }

    @Override
    public int getNextLogIndex() {
        return nextLogIndex;
    }

    @Override
    public List<Entry> subView(int fromIndex) {
        if (isEmpty() || fromIndex > doGetLastLogIndex()) {
            return Collections.emptyList();
        }
        return subList(Math.max(fromIndex, doGetFirstLogIndex()), nextLogIndex);
    }

    @Override
    public List<Entry> subList(int fromIndex) {
        if (isEmpty() || fromIndex > doGetLastLogIndex()) {
            return Collections.emptyList();
        }
        return subList(Math.max(fromIndex, doGetFirstLogIndex()), nextLogIndex);
    }

    @Override
    public List<Entry> subList(int fromIndex, int toIndex) {
        if (isEmpty()) {
            throw new EmptySequenceException();
        }
        if (fromIndex < doGetFirstLogIndex() || toIndex > doGetLastLogIndex() + 1 || fromIndex > toIndex) {
            throw new IllegalArgumentException("Illegal from index " + fromIndex + "or to index" + toIndex);
        }
        return doSubList(fromIndex, toIndex);
    }

    @Override
    public boolean isEntryPresent(int index) {
        return !isEmpty() && index >= doGetFirstLogIndex() && index <= doGetLastLogIndex();
    }

    @Override
    public EntryMetaData getEntryMetaData(int index) {
        Entry entry = getEntry(index);
        return entry != null ? entry.getMetaData() : null;
    }

    @Override
    public Entry getEntry(int index) {
        if (!isEntryPresent(index)) {
            return null;
        }
        return doGetEntry(index);
    }

    @Override
    public Entry getLastEntry() {
        return isEmpty() ? null : doGetEntry(doGetLastLogIndex());
    }

    @Override
    public void append(Entry entry) {
        if (entry.getIndex() != nextLogIndex) {
            // maybe should not happen, i am not sure
            throw new IllegalArgumentException("Entry index must be" + nextLogIndex);
        }
        doAppend(entry);
        nextLogIndex++;
    }

    @Override
    public void append(List<Entry> entries) {
        for (Entry entry : entries) {
            append(entry);
        }
    }

    @Override
    public void removeAfter(int index) {
        if (isEmpty() || index >= doGetLastLogIndex()) {
            // should not happen
            return;
        }
        doRemoveAfter(index);
    }

    protected abstract Entry doGetEntry(int index);

    protected abstract List<Entry> doSubList(int fromIndex, int toIndex);

    protected abstract void doAppend(Entry entry);

    protected abstract void doRemoveAfter(int index);

    int doGetFirstLogIndex() {
        return logIndexOffset;
    }

    int doGetLastLogIndex() {
        return nextLogIndex - 1;
    }
}
