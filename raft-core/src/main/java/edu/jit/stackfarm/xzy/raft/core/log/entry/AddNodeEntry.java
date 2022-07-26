package edu.jit.stackfarm.xzy.raft.core.log.entry;


import edu.jit.stackfarm.xzy.raft.core.Protos;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
public class AddNodeEntry extends GroupConfigEntry {

    private final NodeEndpoint newNodeEndpoint;

    public AddNodeEntry(int index, int term, Set<NodeEndpoint> nodeEndpoints, NodeEndpoint newNodeEndpoint) {
        super(KIND_ADD_NODE, index, term, nodeEndpoints);
        this.newNodeEndpoint = newNodeEndpoint;
    }

    public Set<NodeEndpoint> getResultNodeEndpoints() {
        Set<NodeEndpoint> configs = new HashSet<>(getNodeEndpoints());
        configs.add(newNodeEndpoint);
        return configs;
    }

    @Override
    public byte[] getCommandBytes() {
        return Protos.AddNodeCommand.newBuilder()
                .addAllNodeEndpoints(getNodeEndpoints().stream().map(c ->
                        Protos.NodeEndpoint.newBuilder()
                                .setId(c.getId().getValue())
                                .setHost(c.getAddress().getHost())
                                .setPort(c.getAddress().getPort())
                                .build()
                ).collect(Collectors.toList()))
                .setNewNodeEndpoint(Protos.NodeEndpoint.newBuilder()
                        .setId(newNodeEndpoint.getId().getValue())
                        .setHost(newNodeEndpoint.getAddress().getHost())
                        .setPort(newNodeEndpoint.getAddress().getPort())
                        .build()
                ).build().toByteArray();
    }

    @Override
    public String toString() {
        return "AddNodeEntry{" +
                "index=" + getIndex() +
                ", term=" + getTerm() +
                ", nodeEndpoints=" + getNodeEndpoints() +
                ", newNodeEndpoint=" + newNodeEndpoint +
                '}';
    }

}
