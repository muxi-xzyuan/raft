package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryFactory;
import edu.jit.stackfarm.xzy.raft.core.log.entry.GeneralEntry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.NoOpEntry;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class EntriesFileTest {

    private static RandomAccessFile randomAccessFile;
    private static EntriesFile entriesFile;
    private static File file;

    @BeforeClass
    @SneakyThrows
    public static void setup() {
        file = File.createTempFile("entries", ".bin");
        file.deleteOnExit();
        randomAccessFile = new RandomAccessFile(file, "rw");
        entriesFile = new EntriesFile(randomAccessFile);
    }

    @AfterClass
    @SneakyThrows
    public static void close() {
        randomAccessFile.close();
    }

    @Before
    @SneakyThrows
    public void reset() {
        randomAccessFile.seek(0);
        randomAccessFile.setLength(0);
    }

    @Test
    public void testAppendEntry() throws IOException {
        assertEquals(0L, EntriesFileTest.entriesFile.appendEntry(new NoOpEntry(2, 3)));
        randomAccessFile.seek(0);
        //kind
        assertEquals(Entry.KIND_NO_OP, randomAccessFile.readInt());
        //index
        assertEquals(2, randomAccessFile.readInt());
        //term
        assertEquals(3, randomAccessFile.readInt());
        //command bytes length
        assertEquals(0, randomAccessFile.readInt());

        byte[] commandBytes = "test".getBytes();
        assertEquals(16L, EntriesFileTest.entriesFile.appendEntry(new GeneralEntry(3, 3, commandBytes)));
        randomAccessFile.seek(16L);
        //kind
        assertEquals(Entry.KIND_GENERAL, randomAccessFile.readInt());
        //index
        assertEquals(3, randomAccessFile.readInt());
        //term
        assertEquals(3, randomAccessFile.readInt());
        //command bytes length
        assertEquals(4, randomAccessFile.readInt());
        byte[] buffer = new byte[4];
        randomAccessFile.read(buffer);
        assertArrayEquals(commandBytes, buffer);
    }

    @Test
    public void testLoadEntry() throws IOException {
        assertEquals(0L, entriesFile.appendEntry(new NoOpEntry(2, 3)));
        assertEquals(16L, entriesFile.appendEntry(new GeneralEntry(3, 3, "test".getBytes())));
        assertEquals(36L, entriesFile.appendEntry(new GeneralEntry(4, 3, "foo".getBytes())));

        EntryFactory factory = new EntryFactory();

        Entry entry = entriesFile.loadEntry(0, factory);
        assertEquals(entry.getKind(), Entry.KIND_NO_OP);
        assertEquals(entry.getIndex(), 2);
        assertEquals(entry.getTerm(), 3);

        entry = entriesFile.loadEntry(16, factory);
        assertEquals(entry.getKind(), Entry.KIND_GENERAL);
        assertEquals(entry.getIndex(), 3);
        assertEquals(entry.getTerm(), 3);
        assertArrayEquals(entry.getCommandBytes(), "test".getBytes());

        entry = entriesFile.loadEntry(36, factory);
        assertEquals(entry.getKind(), Entry.KIND_GENERAL);
        assertEquals(entry.getIndex(), 4);
        assertEquals(entry.getTerm(), 3);
        assertArrayEquals(entry.getCommandBytes(), "foo".getBytes());
    }
}