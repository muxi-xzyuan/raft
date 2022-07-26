package edu.jit.stackfarm.xzy.raft.kvstore.client;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.service.AddNodeCommand;
import edu.jit.stackfarm.xzy.raft.core.service.RemoveNodeCommand;
import edu.jit.stackfarm.xzy.raft.core.service.ServerRouter;
import edu.jit.stackfarm.xzy.raft.kvstore.message.GetCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.message.SetCommand;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class Client {

    public static final String VERSION = "0.0.1";
    @Getter
    private final ServerRouter serverRouter;


    public void addNode(String nodeId, String host, int port) {
        serverRouter.send(new AddNodeCommand(nodeId, host, port));
    }

    public void removeNode(String nodeId) {
        serverRouter.send(new RemoveNodeCommand(NodeId.of(nodeId)));
    }

    public void updateNode(String nodeId, String host, int port) {
        serverRouter.send(new RemoveNodeCommand(NodeId.of(nodeId)));
        serverRouter.send(new AddNodeCommand(nodeId, host, port));
    }

    public void set(String key, byte[] value) {
        serverRouter.send(new SetCommand(key, value));
    }

    public byte[] get(String key) {
        return (byte[]) serverRouter.send(new GetCommand(key));
    }

}
