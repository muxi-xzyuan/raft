package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryMetaData;
import edu.jit.stackfarm.xzy.raft.core.log.entry.NoOpEntry;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class MemoryEntrySequenceTest {

    @Test
    public void testAppendEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence();
        sequence.append(new NoOpEntry(sequence.getNextLogIndex(), 1));
        assertEquals(2, sequence.getNextLogIndex());
        assertEquals(1, sequence.getLastLogIndex());
    }

    @Test
    public void testGetEntry() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        assertNull(sequence.getEntry(1));
        assertEquals(2, sequence.getEntry(2).getIndex());
        assertEquals(3, sequence.getEntry(3).getIndex());
        assertNull(sequence.getEntry(4));
    }

    @Test
    public void testGetEntryMetaData() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        assertNull(sequence.getEntry(2));
        sequence.append(new NoOpEntry(2, 1));
        EntryMetaData metaData = sequence.getEntryMetaData(2);
        assertNotNull(metaData);
        assertEquals(2, metaData.getIndex());
        assertEquals(1, metaData.getTerm());
    }

    @Test
    public void testSubListOneElement() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        List<Entry> subList = sequence.subList(2, 3);
        assertEquals(1, subList.size());
        assertEquals(2, subList.get(0).getIndex());
    }

    @Test
    public void testRemoveAfterPartial() {
        MemoryEntrySequence sequence = new MemoryEntrySequence(2);
        sequence.append(Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 1)
        ));
        sequence.removeAfter(2);
        assertEquals(2, sequence.getLastLogIndex());
        assertEquals(2, sequence.getLastLogIndex());
        assertEquals(3, sequence.getNextLogIndex());
    }
}