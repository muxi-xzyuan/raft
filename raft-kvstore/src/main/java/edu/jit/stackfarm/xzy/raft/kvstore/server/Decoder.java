package edu.jit.stackfarm.xzy.raft.kvstore.server;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.service.AddNodeCommand;
import edu.jit.stackfarm.xzy.raft.core.service.RemoveNodeCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.Protos;
import edu.jit.stackfarm.xzy.raft.kvstore.message.GetCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.message.MessageConstants;
import edu.jit.stackfarm.xzy.raft.kvstore.message.SetCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class Decoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 8) return;

        in.markReaderIndex();
        int messageType = in.readInt();
        int payloadLength = in.readInt();
        if (in.readableBytes() < payloadLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] payload = new byte[payloadLength];
        in.readBytes(payload);
        switch (messageType) {
            case MessageConstants.MSG_TYPE_ADD_SERVER_COMMAND:
                Protos.AddNodeCommand protoAddServerCommand = Protos.AddNodeCommand.parseFrom(payload);
                out.add(new AddNodeCommand(protoAddServerCommand.getNodeId(), protoAddServerCommand.getHost(), protoAddServerCommand.getPort()));
                break;
            case MessageConstants.MSG_TYPE_REMOVE_SERVER_COMMAND:
                Protos.RemoveNodeCommand protoRemoveServerCommand = Protos.RemoveNodeCommand.parseFrom(payload);
                out.add(new RemoveNodeCommand(NodeId.of(protoRemoveServerCommand.getNodeId())));
                break;
            case MessageConstants.MSG_TYPE_GET_COMMAND:
                Protos.GetCommand protoGetCommand = Protos.GetCommand.parseFrom(payload);
                out.add(new GetCommand(protoGetCommand.getKey()));
                break;
            case MessageConstants.MSG_TYPE_SET_COMMAND:
                Protos.SetCommand protoSetCommand = Protos.SetCommand.parseFrom(payload);
                out.add(new SetCommand(protoSetCommand.getKey(), protoSetCommand.getValue().toByteArray()));
                break;
            default:
                throw new IllegalStateException("Unexpected message type " + messageType);
        }
    }
}
