package edu.jit.stackfarm.xzy.raft.core.node.role;

/**
 * 节点角色<br>
 * 1: follower<br>
 * 2: candidate<br>
 * 3: leader
 */
public enum NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
