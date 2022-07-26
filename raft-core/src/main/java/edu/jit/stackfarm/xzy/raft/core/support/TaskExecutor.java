package edu.jit.stackfarm.xzy.raft.core.support;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * 任务执行器，简化版ExecutorService
 * 其实没什么必要 TODO Remove
 */
public interface TaskExecutor {

    /**
     * 提交任务，无返回值
     *
     * @param task
     */
    void execute(Runnable task);

    /**
     * 提交任务，有返回值
     *
     * @param task
     * @param <V>
     * @return
     */
    <V> Future<V> submit(Callable<V> task);

    /**
     * 关闭任务执行器
     */
    void shutdown();
}
