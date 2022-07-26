package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 日志数据块，并非用于传输，只是读取数据时的中间产物
 *
 * @see Snapshot#readData
 */
@AllArgsConstructor
public class SnapshotChunk {

    private final byte[] bytes;
    @Getter
    private final boolean lastChunk;

    public byte[] toByteArray() {
        return bytes;
    }

}
