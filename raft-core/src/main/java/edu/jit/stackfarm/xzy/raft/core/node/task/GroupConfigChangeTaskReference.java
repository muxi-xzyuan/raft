package edu.jit.stackfarm.xzy.raft.core.node.task;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeoutException;

/**
 * Reference for group config change task.
 */
public interface GroupConfigChangeTaskReference {

    /**
     * Wait for result forever.
     *
     * @return result
     * @throws InterruptedException if interrupted
     */
    @Nonnull
    GroupConfigChangeTaskResult getResult() throws InterruptedException;

    /**
     * Wait for result in specified timeout.
     *
     * @param timeout timeout
     * @return result
     * @throws InterruptedException if interrupted
     * @throws TimeoutException     if timeout
     */
    @Nonnull
    GroupConfigChangeTaskResult getResult(long timeout) throws InterruptedException, TimeoutException;

    /**
     * Cancel task.
     */
    void cancel();

    /**
     * Wait the task to be done in specified timeout. if timeout is zero, wait forever.
     *
     * @param timeout milliseconds, if timeout is zero, wait forever
     * @throws TimeoutException     if timeout
     * @throws InterruptedException if interrupted
     */
    default void awaitDone(long timeout) throws TimeoutException, InterruptedException {
        if (timeout == 0) {
            getResult();
        } else {
            getResult(timeout);
        }
    }

}
