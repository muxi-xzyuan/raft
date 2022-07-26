package edu.jit.stackfarm.xzy.raft.core.rpc.nio;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.MessageConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtobufEncoderTest {

    @Test
    public void testNodeId() throws Exception {
        ProtobufEncoder encoder = new ProtobufEncoder();
        ByteBuf buffer = Unpooled.buffer();
        encoder.encode(null, NodeId.of("A"), buffer);
        assertEquals(MessageConstants.MSG_TYPE_NODE_ID, buffer.readInt());
        assertEquals(1, buffer.readInt());
        assertEquals((byte) 'A', buffer.readByte());
    }
}