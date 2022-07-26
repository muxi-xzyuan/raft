package edu.jit.stackfarm.xzy.raft.core.support;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * 异步单线程任务执行器
 */
@Slf4j
public class SingleThreadTaskExecutor implements TaskExecutor {

    private final ExecutorService executorService;

    public SingleThreadTaskExecutor() {
        executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * 允许设置线程名称的构造函数，方便调试
     *
     * @param threadName 线程名称
     */
    public SingleThreadTaskExecutor(String threadName) {
        this(r -> {
            Thread thread = new Thread(r, threadName);
            thread.setUncaughtExceptionHandler((t, e) -> log.warn("Uncaught exception", e));
            return thread;
        });
    }

    public SingleThreadTaskExecutor(ThreadFactory threadFactory) {
        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void execute(Runnable task) {
        executorService.execute(task);
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        return executorService.submit(task);
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
