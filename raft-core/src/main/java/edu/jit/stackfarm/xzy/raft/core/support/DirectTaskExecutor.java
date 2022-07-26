package edu.jit.stackfarm.xzy.raft.core.support;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * 同步任务执行器
 */
public class DirectTaskExecutor implements TaskExecutor {

    @Override
    public void execute(Runnable task) {
        FutureTask<?> futureTask = new FutureTask<>(task, null);
        futureTask.run();
        try {
            futureTask.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <V> Future<V> submit(Callable<V> task) {
        FutureTask<V> futureTask = new FutureTask<>(task);
        futureTask.run();
        try {
            futureTask.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return futureTask;
    }

//    @Override
//    public void execute(Runnable runnable) {
//
//    }

    @Override
    public void shutdown() {
    }
}
