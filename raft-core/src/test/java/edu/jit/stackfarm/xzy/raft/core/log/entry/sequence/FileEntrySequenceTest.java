package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.NoOpEntry;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FileEntrySequenceTest {

    private static EntriesFile entriesFile;
    private static EntryIndexFile entryIndexFile;
    private static FileEntrySequence fileEntrySequence;
    private static File binFile;
    private static File idxFile;

    @BeforeClass
    @SneakyThrows
    public static void setup() {
        binFile = File.createTempFile("entries", ".bin");
        idxFile = File.createTempFile("entries", "idx");
        binFile.deleteOnExit();
        idxFile.deleteOnExit();
        entriesFile = new EntriesFile(new RandomAccessFile(binFile, "rw"));
        entryIndexFile = new EntryIndexFile(new RandomAccessFile(idxFile, "rw"));

    }

    @AfterClass
    @SneakyThrows
    public static void close() {
        entriesFile.close();
        entryIndexFile.close();
    }

    @Before
    @SneakyThrows
    public void reset() {
        entryIndexFile.clear();
        entriesFile.clear();
        fileEntrySequence = new FileEntrySequence(entriesFile, entryIndexFile, 1);
    }

    @Test
    public void testInitialize() throws IOException {
        entryIndexFile.appendEntryIndex(1, 0L, Entry.KIND_GENERAL, 1);
        entryIndexFile.appendEntryIndex(2, 20L, Entry.KIND_GENERAL, 1);
        assertEquals(3, fileEntrySequence.getNextLogIndex());
        assertEquals(1, fileEntrySequence.getFirstLogIndex());
        assertEquals(2, fileEntrySequence.getLastLogIndex());
        assertEquals(2, fileEntrySequence.getCommitIndex());
    }

    @Test
    public void testAppendEntry() {
        assertEquals(1, fileEntrySequence.getNextLogIndex());
        fileEntrySequence.append(new NoOpEntry(1, 1));
        assertEquals(2, fileEntrySequence.getNextLogIndex());
        assertEquals(1, fileEntrySequence.getLastEntry().getIndex());
    }

    @Test
    public void testGetEntry() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        fileEntrySequence.append(new NoOpEntry(2, 1));
        assertNull(fileEntrySequence.getEntry(0));
        assertEquals(1, fileEntrySequence.getEntry(1).getIndex());
        assertEquals(2, fileEntrySequence.getEntry(2).getIndex());
        assertNull(fileEntrySequence.getEntry(3));
    }

    @Test
    public void testSubList2() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        appendEntryToFile(new NoOpEntry(2, 2));
        fileEntrySequence.append(new NoOpEntry(fileEntrySequence.getNextLogIndex(), 3));
        fileEntrySequence.append(new NoOpEntry(fileEntrySequence.getNextLogIndex(), 4));
        List<Entry> subList = fileEntrySequence.subList(2);
        assertEquals(3, subList.size());
        assertEquals(2, subList.get(0).getIndex());
        assertEquals(4, subList.get(2).getIndex());
    }

    @Test
    public void testRemoveAfterEntriesInFile2() throws IOException {
        appendEntryToFile(new NoOpEntry(1, 1));
        appendEntryToFile(new NoOpEntry(2, 1));
        fileEntrySequence.append(new NoOpEntry(3, 2));
        assertEquals(1, fileEntrySequence.getFirstLogIndex());
        assertEquals(3, fileEntrySequence.getLastLogIndex());
        fileEntrySequence.removeAfter(1);
        assertEquals(1, fileEntrySequence.getFirstLogIndex());
        assertEquals(3, fileEntrySequence.getLastLogIndex());
    }

//    @Test
//    public void testBuildGroupConfigEntryList() throws IOException {
//        Set<NodeEndpoint> nodeEndpoints = new HashSet<>();
//        nodeEndpoints.add(new NodeEndpoint("A", "localhost", 2333));
//        appendEntryToFile(new AddNodeEntry(1, 1, nodeEndpoints, new NodeEndpoint("B", "localhost", 2334)));
//        fileEntrySequence.buildGroupConfigEntryList(nodeEndpoints).subList(Integer.MIN_VALUE, Integer.MAX_VALUE);
//    }

    private void appendEntryToFile(Entry entry) throws IOException {
        long offset = entriesFile.appendEntry(entry);
        entryIndexFile.appendEntryIndex(entry.getIndex(), offset, entry.getKind(), entry.getTerm());
    }
}