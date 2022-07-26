package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.LogDir;
import edu.jit.stackfarm.xzy.raft.core.log.LogException;
import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryFactory;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryMetaData;
import edu.jit.stackfarm.xzy.raft.core.log.entry.GroupConfigEntry;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

/**
 * 保存在文件中的日志条目序列，分为已写入文件部分（一定是已提交的）和待写入文件部分（还未持久化，可能部分是已提交状态）
 */
@Slf4j
public class FileEntrySequence extends AbstractEntrySequence {

    private final EntryFactory entryFactory = new EntryFactory();
    private final EntriesFile entriesFile;
    private final EntryIndexFile entryIndexFile;
    private final LinkedList<Entry> pendingEntries = new LinkedList<>();

    public FileEntrySequence(LogDir logDir, int logIndexOffset) {
        //默认logIndexOffset由外部决定
        super(logIndexOffset);
        try {
            this.entriesFile = new EntriesFile(logDir.getEntriesFile());
            this.entryIndexFile = new EntryIndexFile(logDir.getEntryIndexFile());
            initialize();
        } catch (IOException e) {
            throw new LogException("Failed to open entries file or entry index file", e);
        }
    }

    public FileEntrySequence(EntriesFile entriesFile, EntryIndexFile entryIndexFile, int logIndexOffset) {
        //默认logIndexOffset由外部决定
        super(logIndexOffset);
        this.entriesFile = entriesFile;
        this.entryIndexFile = entryIndexFile;
        initialize();
    }

    @Override
    public GroupConfigEntryList buildGroupConfigEntryList(Set<NodeEndpoint> initialGroup) {
        GroupConfigEntryList list = new GroupConfigEntryList(initialGroup);

        // check file
        try {
            int entryKind;
            for (EntryIndexItem indexItem : entryIndexFile) {
                entryKind = indexItem.getKind();
                if (entryKind == Entry.KIND_ADD_NODE || entryKind == Entry.KIND_REMOVE_NODE) {
                    list.add((GroupConfigEntry) entriesFile.loadEntry(indexItem.getOffset(), entryFactory));
                }
            }
        } catch (IOException e) {
            throw new LogException("Failed to load entry", e);
        }

        // check pending entries
        for (Entry entry : pendingEntries) {
            if (entry instanceof GroupConfigEntry) {
                list.add((GroupConfigEntry) entry);
            }
        }
        return list;
    }

    @Override
    public EntryMetaData getEntryMetaData(int index) {
        if (!isEntryPresent(index)) {
            return null;
        }
        if (!pendingEntries.isEmpty()) {
            int firstPendingEntryIndex = pendingEntries.getFirst().getIndex();
            if (index >= firstPendingEntryIndex) {
                return pendingEntries.get(index - firstPendingEntryIndex).getMetaData();
            }
        }
        return entryIndexFile.get(index).toEntryMetaData();
    }

    @Override
    protected Entry doGetEntry(int index) {
        if (!pendingEntries.isEmpty()) {
            int firstPendingEntryIndex = pendingEntries.getFirst().getIndex();
            if (index >= firstPendingEntryIndex) {
                return pendingEntries.get(index - firstPendingEntryIndex);
            }
        }
        assert !entryIndexFile.isEmpty();
        return getEntryInFile(index);
    }

    @Override
    protected List<Entry> doSubList(int fromIndex, int toIndex) {
        //结果分为来自文件的与来自缓冲的两部分
        List<Entry> result = new ArrayList<>();
        //从文件中获取日志条目
        if (!entryIndexFile.isEmpty() && fromIndex <= entryIndexFile.getMaxEntryIndex()) {
            int maxIndex = Math.min(entryIndexFile.getMaxEntryIndex() + 1, toIndex);
            for (int i = fromIndex; i < maxIndex; i++) {
                result.add(getEntryInFile(i));
            }
        }

        //从日志缓冲中获取日志条目
        if (!pendingEntries.isEmpty() && toIndex > pendingEntries.getFirst().getIndex()) {
            Iterator<Entry> iterator = pendingEntries.iterator();
            Entry entry;
            int index;
            while (iterator.hasNext()) {
                entry = iterator.next();
                index = entry.getIndex();
                if (index >= toIndex) {
                    break;
                }
                if (index >= fromIndex) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    @Override
    protected void doAppend(Entry entry) {
        pendingEntries.add(entry);
    }

    @Override
    protected void doRemoveAfter(int index) {
        if (!pendingEntries.isEmpty() && index >= pendingEntries.getFirst().getIndex() - 1) {
            // remove last n entries in pending entries
            for (int i = index + 1; i <= doGetLastLogIndex(); i++) {
                pendingEntries.removeLast();
            }
            nextLogIndex = index + 1;
            return;
        }
        try {
            if (index >= doGetFirstLogIndex()) {
                pendingEntries.clear();
                // remove entries whose index >= (index + 1)
                entriesFile.truncate(entryIndexFile.getOffset(index + 1));
                entryIndexFile.removeAfter(index);
                nextLogIndex = index + 1;
                commitIndex = index;
            } else {
                pendingEntries.clear();
                entriesFile.clear();
                entryIndexFile.clear();
                nextLogIndex = logIndexOffset;
                commitIndex = logIndexOffset - 1;
            }
        } catch (IOException e) {
            throw new LogException(e);
        }
    }

    @Override
    public void commit(int index) {
        if (index < commitIndex) {
            throw new IllegalArgumentException("commit index < " + commitIndex);
        }
        if (index == commitIndex) {
            return;
        }
        //如果commitIndex在文件内，则只更新commitIndex
        if (!entryIndexFile.isEmpty() && index <= entryIndexFile.getMaxEntryIndex()) {
            commitIndex = index;
            return;
        }

        //如果commitIndex不在日志缓冲的区间内则抛出异常
        if (pendingEntries.isEmpty() ||
                pendingEntries.getFirst().getIndex() > index ||
                pendingEntries.getLast().getIndex() < index) {
            throw new IllegalArgumentException("No entry to commit or commit index exceed");
        }
        long offset;
        Entry entry = null;
        //将缓冲区内的被提交的日志条目写入文件
        try {
            for (int i = pendingEntries.getFirst().getIndex(); i <= index; i++) {
                entry = pendingEntries.removeFirst();
                offset = entriesFile.appendEntry(entry);
                entryIndexFile.appendEntryIndex(i, offset, entry.getKind(), entry.getTerm());
                commitIndex = i;
            }
        } catch (IOException e) {
            throw new LogException("Failed to commit entry " + entry, e);
        }
    }

    @Override
    public void close() {
        try {
            entriesFile.close();
            entryIndexFile.close();
        } catch (IOException e) {
            throw new LogException("Failed to close", e);
        }
    }

    private void initialize() {
        if (entryIndexFile.isEmpty()) {
            commitIndex = logIndexOffset - 1;
            return;
        }
        //使用日志索引文件的minEntryIndex作为logIndexOffset
        logIndexOffset = entryIndexFile.getMinEntryIndex();
        //使用日志索引文件的maxEntryIndex加1作为nextLogIndex
        nextLogIndex = entryIndexFile.getMaxEntryIndex() + 1;
        commitIndex = entryIndexFile.getMaxEntryIndex();
    }

    private Entry getEntryInFile(int index) {
        long offset = entryIndexFile.getOffset(index);
        try {
            return entriesFile.loadEntry(offset, entryFactory);
        } catch (IOException e) {
            throw new LogException("Failed to load entry " + index, e);
        }
    }
}
