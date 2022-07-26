package edu.jit.stackfarm.xzy.raft.core.rpc;

import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AbstractRpcMessage;
import lombok.Data;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class MockConnector implements Connector {

    private final LinkedList<Message> messages = new LinkedList<>();

    @Override
    public void initialize() {
    }

    @Override
    public void send(@NonNull Object rpc, @NonNull NodeEndpoint targetEndpoint) {
        Message message = new Message();
        message.rpc = rpc;
        message.destinationNodeId = targetEndpoint.getId();
        messages.add(message);
    }

    @Override
    public void send(@NonNull Object rpc, @NonNull Collection<NodeEndpoint> targetEndpoints) {
        Message message = new Message();
        message.rpc = rpc;
        messages.add(message);
    }

    @Override
    public void reply(@NonNull Object result, @NonNull AbstractRpcMessage<?> rpcMessage) {
        Message m = new Message();
        m.result = result;
        m.destinationNodeId = rpcMessage.getSourceNodeId();
        messages.add(m);
    }

    @Override
    public void close() {
    }

    //=============================   methods serve for test   =============================
    public Message getLastMessage() {
        return messages.isEmpty() ? null : messages.getLast();
    }

    public Message getLastMessageOrDefault() {
        return messages.isEmpty() ? new Message() : messages.getLast();
    }

    public Object getRpc() {
        return getLastMessageOrDefault().getRpc();
    }

    public Object getResult() {
        return getLastMessageOrDefault().getResult();
    }

    public NodeId getDestinationNodeId() {
        return getLastMessageOrDefault().getDestinationNodeId();
    }

    public int getMessageCount() {
        return messages.size();
    }

    public List<Message> getMessages() {
        return new ArrayList<>(messages);
    }

    @Data
    public static class Message {

        private Object rpc;

        private NodeId destinationNodeId;

        private Object result;
    }
}
