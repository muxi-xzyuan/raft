package edu.jit.stackfarm.xzy.raft.core.log;


import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@Data
@RequiredArgsConstructor
public class AppendEntriesState {

    public static final AppendEntriesState FAILED = new AppendEntriesState(false, null);
    public static final AppendEntriesState SUCCESS = new AppendEntriesState(true, null);

    private final boolean success;
    private final Set<NodeEndpoint> latestGroup;

    public AppendEntriesState(Set<NodeEndpoint> latestGroup) {
        this(true, latestGroup);
    }

    public boolean hasGroup() {
        return latestGroup != null;
    }
}
