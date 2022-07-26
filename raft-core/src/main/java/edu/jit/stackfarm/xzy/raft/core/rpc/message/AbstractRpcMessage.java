package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;
import lombok.Data;

@Data
public abstract class AbstractRpcMessage<T> {

    private final T rpc;
    private final NodeId sourceNodeId;
    private final Channel channel;

}
