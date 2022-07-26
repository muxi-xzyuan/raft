package edu.jit.stackfarm.xzy.raft.core.schedule;

import edu.jit.stackfarm.xzy.raft.core.node.NodeConfig;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@ThreadSafe
public class DefaultScheduler implements Scheduler {

    //最小选举超时时间
    private final int minElectionTimeout;
    //最大选举超时时间
    private final int maxElectionTimeout;
    //初次日志复制延迟时间
    private final int logReplicationDelay;
    //日志复制间隔
    private final int logReplicationInterval;
    //随机数生成器
    private final Random electionTimeoutRandom;

    private final ScheduledExecutorService scheduledExecutorService;

    public DefaultScheduler(NodeConfig config) {
        this(config.getMinElectionTimeout(), config.getMaxElectionTimeout(),
                config.getLogReplicationDelay(), config.getLogReplicationInterval());
    }

    public DefaultScheduler(int minElectionTimeout, int maxElectionTimeout,
                            int logReplicationDelay, int logReplicationInterval) {
        //判断参数是否有效
        //最小和最大选举超时间隔
        if (minElectionTimeout <= 0 || maxElectionTimeout <= 0
                || minElectionTimeout > maxElectionTimeout) {
            throw new IllegalStateException("election timeout should not be less than 0 or min > max");
        }
        //初次日志复制延迟以及日志复制间隔
        if (logReplicationDelay < 0 || logReplicationInterval <= 0) {
            throw new IllegalStateException("log replication delay < 0 or log replication interval <= 0");
        }
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.logReplicationDelay = logReplicationDelay;
        this.logReplicationInterval = logReplicationInterval;
        electionTimeoutRandom = new Random();
        //设置定时器线程名为 scheduler，方便调试
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "scheduler"));
    }

    @Override
    @Nonnull
    public LogReplicationTask scheduleLogReplicationTask(@NonNull Runnable task) {
        log.debug("Schedule log replication task");
        ScheduledFuture<?> scheduledFuture =
                scheduledExecutorService.scheduleWithFixedDelay(task, logReplicationDelay, logReplicationInterval, TimeUnit.MILLISECONDS);
        return new LogReplicationTask(scheduledFuture);
    }

    @Nonnull
    @Override
    public ElectionTimeout scheduleElectionTimeout(@NonNull Runnable task) {
        log.debug("Schedule election timeout");
        //为了减少 split vote 的影响，在选举超时区间内随机选择一个超时时间，而不是固定的选举超时
        int timeout = electionTimeoutRandom.nextInt(maxElectionTimeout - minElectionTimeout) + minElectionTimeout;
        ScheduledFuture<?> scheduledFuture = scheduledExecutorService.schedule(task, timeout, TimeUnit.MILLISECONDS);
        return new ElectionTimeout(scheduledFuture);
    }

    @Override
    public void stop() {
        log.debug("Stopping scheduler");
        scheduledExecutorService.shutdown();
    }

}
