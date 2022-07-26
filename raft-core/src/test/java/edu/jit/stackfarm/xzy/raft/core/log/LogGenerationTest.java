package edu.jit.stackfarm.xzy.raft.core.log;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class LogGenerationTest {

    @Test
    public void isValidDirName() {
        assertTrue(LogGeneration.isValidDirName("log-0"));
        assertTrue(LogGeneration.isValidDirName("log-12"));
        assertTrue(LogGeneration.isValidDirName("log-123"));
        assertFalse(LogGeneration.isValidDirName("log-"));
        assertFalse(LogGeneration.isValidDirName("foo"));
        assertFalse(LogGeneration.isValidDirName("foo-0"));
    }

    @Test
    public void testCreateFromFile() {
        LogGeneration generation = new LogGeneration(new File("log-6"));
        assertEquals(6, generation.getLastIncludedIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromFileFailed() {
        new LogGeneration(new File("foo-6"));
    }

    @Test
    public void testCreateWithBaseDir() {
        LogGeneration generation = new LogGeneration(new File("data"), 10);
        assertEquals(10, generation.getLastIncludedIndex());
        assertEquals("log-10", generation.get().getName());
    }

    @Test
    public void testCompare() {
        File baseDir = new File("data");
        LogGeneration generation = new LogGeneration(baseDir, 10);
        assertEquals(1, generation.compareTo(new LogGeneration(baseDir, 9)));
        assertEquals(0, generation.compareTo(new LogGeneration(baseDir, 10)));
        assertEquals(-1, generation.compareTo(new LogGeneration(baseDir, 11)));
    }

    @Test
    public void testGetFile() {
        LogGeneration generation = new LogGeneration(new File("data"), 20);
        assertEquals(RootDir.FILE_NAME_SNAPSHOT, generation.getSnapshotFile().getName());
        assertEquals(RootDir.FILE_NAME_ENTRIES, generation.getEntriesFile().getName());
        assertEquals(RootDir.FILE_NAME_ENTRY_INDEX, generation.getEntryIndexFile().getName());
    }

}