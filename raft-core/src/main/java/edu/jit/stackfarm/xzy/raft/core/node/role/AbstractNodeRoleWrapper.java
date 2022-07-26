package edu.jit.stackfarm.xzy.raft.core.node.role;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;
import lombok.NonNull;

import javax.annotation.Nullable;

@Data
public abstract class AbstractNodeRoleWrapper {
    //角色任期，逻辑时钟，全局递增
    protected final int term;
    //角色名：leader/follower/candidate
    @NonNull
    private final NodeRole role;

    @Nullable
    public abstract NodeId getLeaderId(NodeId selfId);

    //取消超时或者定时任务
    //通常来说都是取消选举超时，取消日志复制任务的情况是当leader节点心跳超时时，
    //其余节点选出另一leader，原leader节点网络恢复之后日志复制过程中发现对方任期比己方大，
    //则退化为follower，并取消日志复制任务
    public abstract void cancelTimeoutOrTask();
}
