package edu.jit.stackfarm.xzy.raft.kvstore.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.service.*;
import edu.jit.stackfarm.xzy.raft.kvstore.Protos;
import edu.jit.stackfarm.xzy.raft.kvstore.message.GetCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.message.MessageConstants;
import edu.jit.stackfarm.xzy.raft.kvstore.message.SetCommand;
import lombok.AllArgsConstructor;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

@AllArgsConstructor
public class SocketChannel implements Channel {

    private final String host;
    private final int port;

    @Override
    public Object send(Object payload) {
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(5000);
            socket.connect(new InetSocketAddress(this.host, this.port));
            //写入消息
            write(socket.getOutputStream(), payload);
            //读取响应
            return read(socket.getInputStream());
        } catch (IOException e) {
            throw new ChannelException("Failed to connect or send or receive", e);
        }
    }

    private Object read(InputStream input) throws IOException {
        DataInputStream dataInput = new DataInputStream(input);
        int messageType = dataInput.readInt();
        int payloadLength = dataInput.readInt();
        byte[] payload = new byte[payloadLength];
        dataInput.readFully(payload);
        switch (messageType) {
            case MessageConstants.MSG_TYPE_SUCCESS:
                return null;
            case MessageConstants.MSG_TYPE_FAILURE:
                Protos.Failure protoFailure = Protos.Failure.parseFrom(payload);
                throw new ChannelException("Error code " + protoFailure.getErrorCode() + ", message " + protoFailure.getMessage());
            case MessageConstants.MSG_TYPE_REDIRECT:
                Protos.Redirect protoRedirect = Protos.Redirect.parseFrom(payload);
                throw new RedirectException(new NodeId(protoRedirect.getLeaderId()));
            case MessageConstants.MSG_TYPE_GET_COMMAND_RESPONSE:
                Protos.GetCommandResponse protoGetCommandResponse = Protos.GetCommandResponse.parseFrom(payload);
                if (!protoGetCommandResponse.getFound()) return null;
                return protoGetCommandResponse.getValue().toByteArray();
            default:
                throw new ChannelException("Unexpected message type " + messageType);
        }
    }

    private void write(OutputStream output, Object payload) throws IOException {
        if (payload instanceof GetCommand) {
            Protos.GetCommand protoGetCommand = Protos.GetCommand.newBuilder().setKey(((GetCommand) payload).getKey()).build();
            this.write(output, MessageConstants.MSG_TYPE_GET_COMMAND, protoGetCommand);
        } else if (payload instanceof SetCommand) {
            SetCommand setCommand = (SetCommand) payload;
            Protos.SetCommand protoSetCommand = Protos.SetCommand.newBuilder()
                    .setKey(setCommand.getKey())
                    .setValue(ByteString.copyFrom(setCommand.getValue())).build();
            this.write(output, MessageConstants.MSG_TYPE_SET_COMMAND, protoSetCommand);
        } else if (payload instanceof AddNodeCommand) {
            AddNodeCommand command = (AddNodeCommand) payload;
            Protos.AddNodeCommand protoAddServerCommand = Protos.AddNodeCommand.newBuilder().setNodeId(command.getNodeId())
                    .setHost(command.getHost()).setPort(command.getPort()).build();
            this.write(output, MessageConstants.MSG_TYPE_ADD_SERVER_COMMAND, protoAddServerCommand);
        } else if (payload instanceof RemoveNodeCommand) {
            RemoveNodeCommand command = (RemoveNodeCommand) payload;
            Protos.RemoveNodeCommand protoRemoveServerCommand = Protos.RemoveNodeCommand.newBuilder().setNodeId(command.getNodeId().getValue()).build();
            this.write(output, MessageConstants.MSG_TYPE_REMOVE_SERVER_COMMAND, protoRemoveServerCommand);
        }
    }

    private void write(OutputStream output, int messageType, MessageLite message) throws IOException {
        DataOutputStream dataOutput = new DataOutputStream(output);
        byte[] messageBytes = message.toByteArray();
        dataOutput.writeInt(messageType);
        dataOutput.writeInt(messageBytes.length);
        dataOutput.write(messageBytes);
        dataOutput.flush();
    }
}
