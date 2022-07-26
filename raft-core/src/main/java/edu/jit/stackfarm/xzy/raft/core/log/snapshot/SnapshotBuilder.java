package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import edu.jit.stackfarm.xzy.raft.core.rpc.message.InstallSnapshotRpc;

/**
 * 日志快照写入接口
 * 日志的写入主要在生成和安装时发生
 */
public interface SnapshotBuilder<T extends Snapshot> {

    /**
     * 追加日志快照内容
     */
    void append(InstallSnapshotRpc rpc);

    /**
     * 导出日志快照
     *
     * @return
     */
    T build();

    /**
     * 关闭日志快照（清理）
     */
    void close();

}
