package edu.jit.stackfarm.xzy.raft.core.rpc.message;


import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import io.netty.channel.Channel;

public class PreVoteRpcMessage extends AbstractRpcMessage<PreVoteRpc> {

    public PreVoteRpcMessage(PreVoteRpc rpc, NodeId sourceNodeId, Channel channel) {
        super(rpc, sourceNodeId, channel);
    }

}
