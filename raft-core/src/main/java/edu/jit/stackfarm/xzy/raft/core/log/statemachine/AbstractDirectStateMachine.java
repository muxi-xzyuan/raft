package edu.jit.stackfarm.xzy.raft.core.log.statemachine;

import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Set;

@Slf4j
@Getter
public abstract class AbstractDirectStateMachine implements StateMachine {

    protected int lastApplied = 0;

    @Override
    public void applyLog(StateMachineContext context, int index, int term, @Nonnull byte[] commandBytes, int firstLogIndex, Set<NodeEndpoint> lastGroupConfig) {
        //忽略已应用过的日志
        if (index <= lastApplied) {
            return;
        }
        log.debug("Apply log {}", index);
        applyCommand(commandBytes);
        lastApplied = index;
    }

    protected abstract void applyCommand(byte[] commandBytes);

}
