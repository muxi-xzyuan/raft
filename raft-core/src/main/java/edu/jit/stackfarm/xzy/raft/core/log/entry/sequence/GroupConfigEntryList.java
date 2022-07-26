package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;


import edu.jit.stackfarm.xzy.raft.core.log.entry.GroupConfigEntry;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.stream.Collectors;

@ToString
@NotThreadSafe
public class GroupConfigEntryList {

    private final Set<NodeEndpoint> initialGroup;
    private final LinkedList<GroupConfigEntry> entries = new LinkedList<>();

    public GroupConfigEntryList(Set<NodeEndpoint> initialGroup) {
        this.initialGroup = Collections.unmodifiableSet(initialGroup);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Nullable
    public GroupConfigEntry getLast() {
        return entries.isEmpty() ? null : entries.getLast();
    }

    public void add(GroupConfigEntry entry) {
        entries.add(entry);
    }

    public Set<NodeEndpoint> getLastGroupBefore(int index) {
        Iterator<GroupConfigEntry> iterator = entries.descendingIterator();
        while (iterator.hasNext()) {
            GroupConfigEntry entry = iterator.next();
            if (entry.getIndex() <= index) {
                return entry.getResultNodeEndpoints();
            }
        }
        return initialGroup;
    }

    /**
     * Remove entries whose index is greater than {@code entryIndex}.
     *
     * @param entryIndex entry index
     * @return first removed entry, {@code null} if no entry removed
     */
    public GroupConfigEntry removeAfter(int entryIndex) {
        Iterator<GroupConfigEntry> iterator = entries.iterator();
        GroupConfigEntry firstRemovedEntry = null;
        while (iterator.hasNext()) {
            GroupConfigEntry entry = iterator.next();
            if (entry.getIndex() > entryIndex) {
                if (firstRemovedEntry == null) {
                    firstRemovedEntry = entry;
                }
                iterator.remove();
            }
        }
        return firstRemovedEntry;
    }

    /**
     * @param fromIndex inclusive
     * @param toIndex   exclusive
     */
    public List<GroupConfigEntry> subList(int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("from index > to index");
        }
        return entries.parallelStream()
                .filter(e -> e.getIndex() >= fromIndex && e.getIndex() < toIndex)
                .collect(Collectors.toList());
    }

}
