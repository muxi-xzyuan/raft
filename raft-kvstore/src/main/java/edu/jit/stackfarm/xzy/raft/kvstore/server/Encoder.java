package edu.jit.stackfarm.xzy.raft.kvstore.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import edu.jit.stackfarm.xzy.raft.kvstore.Protos;
import edu.jit.stackfarm.xzy.raft.kvstore.message.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class Encoder extends MessageToByteEncoder<Object> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        if (msg instanceof Success) {
            writeMessage(MessageConstants.MSG_TYPE_SUCCESS, Protos.Success.newBuilder().build(), out);
        } else if (msg instanceof Failure) {
            Failure failure = (Failure) msg;
            Protos.Failure protoFailure = Protos.Failure.newBuilder().setErrorCode(failure.getErrorCode()).setMessage(failure.getMessage()).build();
            writeMessage(MessageConstants.MSG_TYPE_FAILURE, protoFailure, out);
        } else if (msg instanceof Redirect) {
            Redirect redirect = (Redirect) msg;
            Protos.Redirect protoRedirect = Protos.Redirect.newBuilder().setLeaderId(redirect.getLeaderId().getValue()).build();
            writeMessage(MessageConstants.MSG_TYPE_REDIRECT, protoRedirect, out);
        } else if (msg instanceof GetCommandResponse) {
            GetCommandResponse response = (GetCommandResponse) msg;
            byte[] value = response.getValue();
            Protos.GetCommandResponse protoResponse = Protos.GetCommandResponse.newBuilder()
                    .setFound(response.isFound())
                    .setValue(value != null ? ByteString.copyFrom(value) : ByteString.EMPTY).build();
            writeMessage(MessageConstants.MSG_TYPE_GET_COMMAND_RESPONSE, protoResponse, out);
        }
    }

    private void writeMessage(int messageType, MessageLite message, ByteBuf out) {
        out.writeInt(messageType);
        byte[] bytes = message.toByteArray();
        out.writeInt(bytes.length);
        out.writeBytes(bytes);
    }

}
