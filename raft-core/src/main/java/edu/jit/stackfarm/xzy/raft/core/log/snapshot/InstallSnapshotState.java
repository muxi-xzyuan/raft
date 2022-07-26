package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Getter;

import java.util.Set;

@Getter
public class InstallSnapshotState {

    private final StateName stateName;
    private Set<NodeEndpoint> lastConfig;

    public InstallSnapshotState(StateName stateName) {
        this.stateName = stateName;
    }

    public InstallSnapshotState(StateName stateName, Set<NodeEndpoint> lastConfig) {
        this.stateName = stateName;
        this.lastConfig = lastConfig;
    }

    public enum StateName {
        ILLEGAL_INSTALL_SNAPSHOT_RPC,
        INSTALLING,
        INSTALLED
    }
}
