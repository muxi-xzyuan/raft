package edu.jit.stackfarm.xzy.raft.core.schedule;

import lombok.NonNull;

import javax.annotation.Nonnull;

/**
 * 调度器，创建定时器
 */
public interface Scheduler {

    @Nonnull
    LogReplicationTask scheduleLogReplicationTask(@NonNull Runnable task);

    @Nonnull
    ElectionTimeout scheduleElectionTimeout(@NonNull Runnable task);

    void stop();

}
