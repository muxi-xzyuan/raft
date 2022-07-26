package edu.jit.stackfarm.xzy.raft.core.log;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.NoOpEntry;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AppendEntriesRpc;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MemoryLogTest {

    @Test
    public void testCreateAppendEntriesRpcStartFromOne() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.appendEntry(1);
        AppendEntriesRpc rpc = log.createAppendEntriesRpc(1, NodeId.of("A"), 1, Log.ALL_ENTRIES);
        assertEquals(1, rpc.getTerm());
        assertEquals(0, rpc.getPrevLogIndex());
        assertEquals(0, rpc.getPrevLogTerm());
        assertEquals(2, rpc.getEntries().size());
        assertEquals(1, rpc.getEntries().get(0).getIndex());
    }

    //follower: (1, 1), (2, 1)
    //leader  :         (2, 1), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderSkip() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.appendEntry(1);
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 1),
                new NoOpEntry(3, 2)
        );
//        assertTrue(log.appendEntriesFromLeader(1, 1, leaderEntries));
    }

    //follower: (1, 1), (2, 1)
    //leader  :         (2, 2), (3, 2)
    @Test
    public void testAppendEntriesFromLeaderConflict1() {
        MemoryLog log = new MemoryLog();
        log.appendEntry(1);
        log.appendEntry(1);
        List<Entry> leaderEntries = Arrays.asList(
                new NoOpEntry(2, 2),
                new NoOpEntry(3, 2)
        );
//        assertTrue(log.appendEntriesFromLeader(1, 1, leaderEntries));
    }
}