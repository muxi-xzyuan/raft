package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import edu.jit.stackfarm.xzy.raft.core.log.LogDir;
import edu.jit.stackfarm.xzy.raft.core.log.LogException;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;

import java.io.IOException;

/**
 * Build {@code FileSnapshot} object based on {@code InstallSnapshotRpc} objects from leader.
 */
public class FileSnapshotBuilder extends AbstractSnapshotBuilder<FileSnapshot> {

    private final LogDir logDir;
    private final FileSnapshotWriter writer;

    /**
     * @param firstRpc 第一段日志快照安装Rpc
     */
    public FileSnapshotBuilder(InstallSnapshotRpc firstRpc, LogDir logDir) {
        super(firstRpc);
        this.logDir = logDir;

        try {
            writer = new FileSnapshotWriter(logDir.getSnapshotFile(), firstRpc.getLastIndex(), firstRpc.getLastTerm(), firstRpc.getLastConfig());
            writer.write(firstRpc.getData());
        } catch (IOException e) {
            throw new LogException("Failed to write snapshot data to file", e);
        }
    }

    @Override
    protected void doWrite(byte[] data) throws IOException {
        writer.write(data);
    }

    @Override
    public FileSnapshot build() {
        close();
        return new FileSnapshot(logDir);
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new LogException("Failed to close writer", e);
        }
    }

}
