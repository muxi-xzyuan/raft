package edu.jit.stackfarm.xzy.raft.core.schedule;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 选举超时定时器，只是定时任务的简单包装，暴露了cancel方法
 */
@Slf4j
@RequiredArgsConstructor
public class ElectionTimeout {

    public static final ElectionTimeout TIME_OUT_NEVER = new ElectionTimeout(null) {
        @Override
        public void cancel() {
        }

        @Override
        public String toString() {
            return "ElectionNeverTimeOut";
        }
    };

    private final ScheduledFuture<?> scheduledFuture;

    //取消选举超时
    public void cancel() {
        String result = scheduledFuture.cancel(false) ? "success" : "fail";
        log.debug("Cancel election timeout {}", result);
    }

    @Override
    public String toString() {
        //选举超时已取消
        if (scheduledFuture.isCancelled()) {
            return "ElectionTimeout(state=cancelled)";
        }
        //选举超时已执行
        if (scheduledFuture.isDone()) {
            return "ElectionTimeout(state=done)";
        }
        //选举超时尚未执行，在多少毫秒后执行
        return "ElectionTimeout{delay=" + scheduledFuture.getDelay(TimeUnit.MILLISECONDS) + "ms}";
    }

}
