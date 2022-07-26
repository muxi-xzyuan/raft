package edu.jit.stackfarm.xzy.raft.core.node.task;

import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class AbstractGroupConfigChangeTask implements GroupConfigChangeTask {

    protected final GroupConfigChangeTaskContext context;
    protected State state = State.START;

    AbstractGroupConfigChangeTask(GroupConfigChangeTaskContext context) {
        this.context = context;
    }

    @Override
    public synchronized GroupConfigChangeTaskResult call() throws Exception {
        log.debug("Group config change task start");
        setState(State.START);
        appendGroupConfig();
        wait();
        log.debug("Group config change task done");
        context.done();
        return mapResult(state);
    }

    @Override
    public synchronized void onLogCommitted() {
        if (state != State.GROUP_CONFIG_APPENDED) {
            throw new IllegalStateException("Log committed before log appended");
        }
        setState(State.GROUP_CONFIG_COMMITTED);
        notify();
    }

    private GroupConfigChangeTaskResult mapResult(State state) {
        if (state == State.GROUP_CONFIG_COMMITTED) {
            return GroupConfigChangeTaskResult.OK;
        }
        return GroupConfigChangeTaskResult.TIMEOUT;
    }

    protected void setState(State state) {
        log.debug("State -> {}", state);
        this.state = state;
    }

    protected abstract void appendGroupConfig();

    protected enum State {
        START,
        GROUP_CONFIG_APPENDED,
        GROUP_CONFIG_COMMITTED,
        TIMEOUT
    }
}
