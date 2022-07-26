package edu.jit.stackfarm.xzy.raft.core.node.task;


import java.util.concurrent.Callable;

/**
 * 多个此种任务不可被并发执行，必须走完完整的生命周期后，后一任务才能被执行。
 */
public interface GroupConfigChangeTask extends Callable<GroupConfigChangeTaskResult> {

    /**
     * 日志提交后的回调函数
     */
    void onLogCommitted();

    GroupConfigChangeTask NO_TASK = new GroupConfigChangeTask() {
        @Override
        public GroupConfigChangeTaskResult call() {
            return null;
        }

        @Override
        public void onLogCommitted() {
        }

        @Override
        public String toString() {
            return "NoGroupConfigChangeTask";
        }
    };
}
