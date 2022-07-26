package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;

import static org.junit.Assert.*;

public class EntryIndexFileTest {

    private static RandomAccessFile randomAccessFile;
    private static EntryIndexFile entryIndexFile;
    private static File file;

    @BeforeClass
    @SneakyThrows
    public static void setup() {
        file = File.createTempFile("entries", ".idx");
        file.deleteOnExit();
        randomAccessFile = new RandomAccessFile(file, "rw");
        entryIndexFile = new EntryIndexFile(randomAccessFile);
    }

    @AfterClass
    @SneakyThrows
    public static void close() {
        randomAccessFile.close();
    }

    @Before
    @SneakyThrows
    public void reset() {
        randomAccessFile.setLength(0);
        randomAccessFile.seek(0);
        entryIndexFile.clear();
    }

    private void makeEntryIndexFileContent(int minEntryIndex, int maxEntryIndex) throws IOException {
        randomAccessFile.writeInt(minEntryIndex);
        randomAccessFile.writeInt(maxEntryIndex);
        for (int i = minEntryIndex; i <= maxEntryIndex; i++) {
            //offset
            randomAccessFile.writeLong(10L * i);
            //kind
            randomAccessFile.writeInt(Entry.KIND_GENERAL);
            //term
            randomAccessFile.writeInt(i);
        }
        randomAccessFile.seek(0);
        entryIndexFile = new EntryIndexFile(randomAccessFile);
    }

    @Test
    public void testLoad() throws IOException {
        makeEntryIndexFileContent(3, 4);
        assertEquals(3, entryIndexFile.getMinEntryIndex());
        assertEquals(4, entryIndexFile.getMaxEntryIndex());
        assertEquals(2, entryIndexFile.getEntryIndexCount());
        EntryIndexItem item = entryIndexFile.get(3);
        assertNotNull(item);
        assertEquals(30L, item.getOffset());
        assertEquals(Entry.KIND_GENERAL, item.getKind());
        assertEquals(3, item.getTerm());
        item = entryIndexFile.get(4);
        assertNotNull(item);
        assertEquals(40L, item.getOffset());
        assertEquals(Entry.KIND_GENERAL, item.getKind());
        assertEquals(4, item.getTerm());
    }

    @Test
    public void testAppendEntryIndex() throws IOException {
        entryIndexFile.appendEntryIndex(10, 100L, Entry.KIND_GENERAL, 2);
        assertEquals(1, entryIndexFile.getEntryIndexCount());
        assertEquals(10, entryIndexFile.getMinEntryIndex());
        assertEquals(10, entryIndexFile.getMaxEntryIndex());
        randomAccessFile.seek(0L);
        //min entry index
        assertEquals(10, randomAccessFile.readInt());
        //max entry index
        assertEquals(10, randomAccessFile.readInt());
        //offset
        assertEquals(100L, randomAccessFile.readLong());
        //kind
        assertEquals(1, randomAccessFile.readInt());
        //term
        assertEquals(2, randomAccessFile.readInt());
        EntryIndexItem item = entryIndexFile.get(10);
        assertNotNull(item);
        assertEquals(100L, item.getOffset());
        assertEquals(Entry.KIND_GENERAL, item.getKind());
        assertEquals(2, item.getTerm());
        entryIndexFile.appendEntryIndex(11, 200L, Entry.KIND_GENERAL, 2);
        assertEquals(2, entryIndexFile.getEntryIndexCount());
        assertEquals(10, entryIndexFile.getMinEntryIndex());
        assertEquals(11, entryIndexFile.getMaxEntryIndex());
        randomAccessFile.seek(24L);
        assertEquals(200L, randomAccessFile.readLong());
        assertEquals(1, randomAccessFile.readInt());
        assertEquals(2, randomAccessFile.readInt());
    }

    @Test
    public void testClear() throws IOException {
        makeEntryIndexFileContent(1, 2);
        assertFalse(entryIndexFile.isEmpty());
        entryIndexFile.clear();
        assertTrue(entryIndexFile.isEmpty());
        assertEquals(0, entryIndexFile.getEntryIndexCount());
        assertEquals(0L, randomAccessFile.length());
    }

    @Test
    public void testGet() throws IOException {
        makeEntryIndexFileContent(3, 4);
        EntryIndexItem item = entryIndexFile.get(3);
        assertNotNull(item);
        assertEquals(Entry.KIND_GENERAL, item.getKind());
        assertEquals(3, item.getTerm());
    }

    @Test
    public void testIterator() throws IOException {
        makeEntryIndexFileContent(3, 4);
        Iterator<EntryIndexItem> iterator = entryIndexFile.iterator();
        assertTrue(iterator.hasNext());
        EntryIndexItem item = iterator.next();
        assertEquals(3, item.getIndex());
        assertEquals(Entry.KIND_GENERAL, item.getKind());
        assertEquals(3, item.getTerm());
        assertTrue(iterator.hasNext());
        item = iterator.next();
        assertEquals(4, item.getIndex());
        assertFalse(iterator.hasNext());
    }
}