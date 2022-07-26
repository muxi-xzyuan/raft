package edu.jit.stackfarm.xzy.raft.core.log.snapshot;


import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;

public class DumbSnapshotBuilder implements SnapshotBuilder {

    @Override
    public void append(InstallSnapshotRpc rpc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Snapshot build() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
}
