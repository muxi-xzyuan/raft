package edu.jit.stackfarm.xzy.raft.kvstore.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;

@Data
public class Redirect {

    private final NodeId leaderId;

}
