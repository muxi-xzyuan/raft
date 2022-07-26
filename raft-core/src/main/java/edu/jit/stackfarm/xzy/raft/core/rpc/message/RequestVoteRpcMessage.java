package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;

public class RequestVoteRpcMessage extends AbstractRpcMessage<RequestVoteRpc> {

    public RequestVoteRpcMessage(RequestVoteRpc rpc, NodeId sourceNodeId, Channel channel) {
        super(rpc, sourceNodeId, channel);
    }

}