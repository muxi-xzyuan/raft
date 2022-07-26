package edu.jit.stackfarm.xzy.raft.core.node;

public enum NodeMode {
    // 此节点始终为从节点，不会发起选举
    QUIET,
    // 集群成员模式，正常发起选举
    STANDARD
}
