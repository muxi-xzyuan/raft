package edu.jit.stackfarm.xzy.raft.core.node;

import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachine;
import edu.jit.stackfarm.xzy.raft.core.node.role.NodeRole;
import edu.jit.stackfarm.xzy.raft.core.node.task.GroupConfigChangeTaskReference;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 一致性核心组件接口
 */
public interface Node {

    void start();

    @Nonnull
    NodeRole getNodeRole();

    @Nullable
    NodeId getLeaderId();

    void enqueueReadIndex(@NonNull String requestId);

    void appendLog(@NonNull byte[] commandBytes);

    void registerStateMachine(@NonNull StateMachine stateMachine);

    @Nonnull
    GroupConfigChangeTaskReference addNode(@NonNull NodeEndpoint endpoint) throws NodeAlreadyAddedException;

    @Nonnull
    GroupConfigChangeTaskReference removeNode(@NonNull NodeId nodeId);

    void stop();
}
