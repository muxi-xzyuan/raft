package edu.jit.stackfarm.xzy.raft.core.schedule;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 日志复制任务，只是定时任务的简单包装，暴露了cancel方法
 */
@Slf4j
@RequiredArgsConstructor
public class LogReplicationTask {

    public static final LogReplicationTask NO_OP = new LogReplicationTask(null) {
        @Override
        public void cancel() {
        }

        @Override
        public String toString() {
            return "NoOpLogReplicationTask";
        }
    };

    private final ScheduledFuture<?> scheduledFuture;

    public void cancel() {
        log.debug("Cancel log replication task");
        scheduledFuture.cancel(false);
    }

    @Override
    public String toString() {
        return "LogReplicationTask{delay=" + scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "ms}";
    }

}
