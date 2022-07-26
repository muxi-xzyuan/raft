package edu.jit.stackfarm.xzy.raft.core.schedule;

import javax.annotation.Nonnull;

/**
 * For unit test.
 */
public class NoobScheduler implements Scheduler {

    @Nonnull
    @Override
    public LogReplicationTask scheduleLogReplicationTask(Runnable task) {
        return LogReplicationTask.NO_OP;
    }

    @Nonnull
    @Override
    public ElectionTimeout scheduleElectionTimeout(Runnable task) {
        return ElectionTimeout.TIME_OUT_NEVER;
    }

    @Override
    public void stop() {
    }

}
