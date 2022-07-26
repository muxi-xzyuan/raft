package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import edu.jit.stackfarm.xzy.raft.core.Protos;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.MessageConstants;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.RequestVoteRpc;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProtobufDecoderTest {

    @Test
    public void testNodeId() throws Exception {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(MessageConstants.MSG_TYPE_NODE_ID);
        buffer.writeInt(1);
        buffer.writeByte((byte) 'A');
        ProtobufDecoder decoder = new ProtobufDecoder();
        List<Object> out = new ArrayList<>();
        decoder.decode(null, buffer, out);
        assertEquals(NodeId.of("A"), out.get(0));
    }

    @Test
    public void testRequestVoteRpc() throws Exception {
        Protos.RequestVoteRpc rpc = Protos.RequestVoteRpc.newBuilder()
                .setLastLogIndex(2)
                .setLastLogTerm(1)
                .setTerm(2)
                .setCandidateId("A")
                .build();
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(MessageConstants.MSG_TYPE_REQUEST_VOTE_RPC);
        byte[] rpcBytes = rpc.toByteArray();
        buffer.writeInt(rpcBytes.length);
        buffer.writeBytes(rpcBytes);
        ProtobufDecoder decoder = new ProtobufDecoder();
        List<Object> out = new ArrayList<>();
        decoder.decode(null, buffer, out);
        RequestVoteRpc decodedRpc = (RequestVoteRpc) out.get(0);
        assertEquals(rpc.getLastLogIndex(), decodedRpc.getLastLogIndex());
        assertEquals(rpc.getLastLogTerm(), decodedRpc.getLastLogTerm());
        assertEquals(rpc.getTerm(), decodedRpc.getTerm());
        assertEquals(NodeId.of(rpc.getCandidateId()), decodedRpc.getCandidateId());
    }
}
