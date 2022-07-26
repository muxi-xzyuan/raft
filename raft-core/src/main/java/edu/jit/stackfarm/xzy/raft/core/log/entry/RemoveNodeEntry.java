package edu.jit.stackfarm.xzy.raft.core.log.entry;

import edu.jit.stackfarm.xzy.raft.core.Protos;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Getter;

import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class RemoveNodeEntry extends GroupConfigEntry {

    private final NodeId nodeToRemove;

    public RemoveNodeEntry(int index, int term, Set<NodeEndpoint> nodeEndpoints, NodeId nodeToRemove) {
        super(KIND_REMOVE_NODE, index, term, nodeEndpoints);
        this.nodeToRemove = nodeToRemove;
    }

    public Set<NodeEndpoint> getResultNodeEndpoints() {
        return getNodeEndpoints().stream()
                .filter(c -> !c.getId().equals(nodeToRemove))
                .collect(Collectors.toSet());
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.RemoveNodeCommand.newBuilder()
                .addAllNodeEndpoints(getNodeEndpoints().stream().map(c ->
                        Protos.NodeEndpoint.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getAddress().getHost())
                                .setPort(c.getAddress().getPort())
                                .build()
                ).collect(Collectors.toList()))
                .setNodeToRemove(nodeToRemove.getValue())
                .build().toByteArray();
    }

    @Override
    public String toString() {
        return "RemoveNodeEntry{" +
                "index=" + getIndex() +
                ", term=" + getTerm() +
                ", nodeEndpoints=" + getNodeEndpoints() +
                ", nodeToRemove=" + nodeToRemove +
                '}';
    }

}
