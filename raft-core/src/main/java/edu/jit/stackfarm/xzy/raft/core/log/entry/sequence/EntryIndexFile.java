package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// todo file is redundant
public class EntryIndexFile implements Iterable<EntryIndexItem> {

    public static final int LENGTH_ENTRY_INDEX_ITEM = 16;
    private static final long OFFSET_MAX_ENTRY_INDEX = Integer.BYTES;
    private final RandomAccessFile randomAccessFile;
    private final Map<Integer, EntryIndexItem> entryIndexMap = new HashMap<>();
    @Getter
    private int entryIndexCount;
    @Getter
    private int minEntryIndex;
    @Getter
    private int maxEntryIndex;

    public EntryIndexFile(File file) throws IOException {
        this(new RandomAccessFile(file, "rw"));
    }

    public EntryIndexFile(RandomAccessFile randomAccessFile) throws IOException {
        this.randomAccessFile = randomAccessFile;
        load();
    }

    public void appendEntryIndex(int index, long offset, int kind, int term) throws IOException {
        if (randomAccessFile.length() == 0L) {
            randomAccessFile.writeInt(index);
            minEntryIndex = index;
        } else {
            if (index != maxEntryIndex + 1) {
                throw new IllegalArgumentException("index must be " + (maxEntryIndex + 1) + ", but is " + index);
            }
            randomAccessFile.seek(OFFSET_MAX_ENTRY_INDEX);
        }
        randomAccessFile.writeInt(index);
        maxEntryIndex = index;
        updateEntryIndexCount();
        randomAccessFile.seek(getOffsetOfEntryIndexItem(index));
        randomAccessFile.writeLong(offset);
        randomAccessFile.writeInt(kind);
        randomAccessFile.writeInt(term);
        entryIndexMap.put(index, new EntryIndexItem(index, offset, kind, term));
    }

    public void removeAfter(int newMaxEntryIndex) throws IOException {
        if (isEmpty() || newMaxEntryIndex >= maxEntryIndex) {
            return;
        }
        if (newMaxEntryIndex < minEntryIndex) {
            clear();
            return;
        }
        randomAccessFile.seek(OFFSET_MAX_ENTRY_INDEX);
        randomAccessFile.writeInt(newMaxEntryIndex);
        randomAccessFile.setLength(getOffsetOfEntryIndexItem(newMaxEntryIndex + 1));
        for (int i = newMaxEntryIndex + 1; i <= maxEntryIndex; i++) {
            entryIndexMap.remove(i);
        }
        maxEntryIndex = newMaxEntryIndex;
        entryIndexCount = newMaxEntryIndex - minEntryIndex + 1;
    }

    public void clear() throws IOException {
        randomAccessFile.setLength(0L);
        entryIndexCount = 0;
        entryIndexMap.clear();
    }

    public boolean isEmpty() {
        return entryIndexCount == 0;
    }

    @Override
    public Iterator<EntryIndexItem> iterator() {
        if (isEmpty()) {
            return Collections.emptyIterator();
        }
        return new EntryIndexIterator(entryIndexCount, minEntryIndex);
    }

    public long getOffset(int entryIndex) {
        return get(entryIndex).getOffset();
    }

    public EntryIndexItem get(int entryIndex) {
        if (entryIndex < minEntryIndex || entryIndex > maxEntryIndex) {
            throw new IllegalArgumentException("index < min or index > max");
        }
        return entryIndexMap.get(entryIndex);
    }

    public void close() throws IOException {
        randomAccessFile.close();
    }

    private void load() throws IOException {
        if (randomAccessFile.length() == 0L) {
            entryIndexCount = 0;
            return;
        }
        minEntryIndex = randomAccessFile.readInt();
        maxEntryIndex = randomAccessFile.readInt();
        updateEntryIndexCount();
        long offset;
        int kind, term;
        for (int i = minEntryIndex; i <= maxEntryIndex; i++) {
            offset = randomAccessFile.readLong();
            kind = randomAccessFile.readInt();
            term = randomAccessFile.readInt();
            entryIndexMap.put(i, new EntryIndexItem(i, offset, kind, term));
        }
    }

    private void updateEntryIndexCount() {
        entryIndexCount = maxEntryIndex - minEntryIndex + 1;
    }

    private long getOffsetOfEntryIndexItem(int index) {
        return (index - minEntryIndex) * LENGTH_ENTRY_INDEX_ITEM + Integer.BYTES * 2;
    }

    @AllArgsConstructor
    private class EntryIndexIterator implements Iterator<EntryIndexItem> {

        private final int entryIndexCount;

        private int currentEntryIndex;

        @Override
        public boolean hasNext() {
            checkModification();
            return currentEntryIndex <= maxEntryIndex;
        }

        @Override
        public EntryIndexItem next() {
            checkModification();
            return entryIndexMap.get(currentEntryIndex++);
        }

        private void checkModification() {
            if (this.entryIndexCount != EntryIndexFile.this.entryIndexCount) {
                throw new IllegalStateException("entry index count changed");
            }
        }
    }
}
