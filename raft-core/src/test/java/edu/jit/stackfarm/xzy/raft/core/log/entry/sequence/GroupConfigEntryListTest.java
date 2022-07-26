package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.AddNodeEntry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.GroupConfigEntry;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GroupConfigEntryListTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetLastGroupBefore() {
        Set<NodeEndpoint> initialGroup = new HashSet<>();
        initialGroup.add(new NodeEndpoint("A", "localhost", 2333));
        GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList(initialGroup);
        groupConfigEntryList.add(new AddNodeEntry(2, 1, initialGroup, new NodeEndpoint("B", "localhost", 2334)));
        assertEquals(initialGroup, groupConfigEntryList.getLastGroupBefore(1));
        initialGroup.add(new NodeEndpoint("B", "localhost", 2334));
        assertEquals(initialGroup, groupConfigEntryList.getLastGroupBefore(3));
    }

    @Test
    public void testRemoveAfter() {
        Set<NodeEndpoint> initialGroup = new HashSet<>();
        initialGroup.add(new NodeEndpoint("A", "localhost", 2333));
        GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList(initialGroup);
        GroupConfigEntry groupConfigEntry = new AddNodeEntry(2, 1, initialGroup, new NodeEndpoint("B", "localhost", 2334));
        groupConfigEntryList.add(groupConfigEntry);
        assertNull(groupConfigEntryList.removeAfter(3));
        assertEquals(groupConfigEntry, groupConfigEntryList.removeAfter(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubList() {
        Set<NodeEndpoint> initialGroup = new HashSet<>();
        initialGroup.add(new NodeEndpoint("A", "localhost", 2333));
        GroupConfigEntryList groupConfigEntryList = new GroupConfigEntryList(initialGroup);
        GroupConfigEntry groupConfigEntry = new AddNodeEntry(2, 1, initialGroup, new NodeEndpoint("B", "localhost", 2334));
        groupConfigEntryList.add(groupConfigEntry);
        assertEquals(Collections.singletonList(groupConfigEntry), groupConfigEntryList.subList(2, 3));
        groupConfigEntryList.subList(2, 1);
    }
}