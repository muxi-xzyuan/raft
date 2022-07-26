package edu.jit.stackfarm.xzy.raft.core.node;

import edu.jit.stackfarm.xzy.raft.core.rpc.Address;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import javax.annotation.concurrent.Immutable;

@Data
@Immutable
@AllArgsConstructor
public class NodeEndpoint {

    @NonNull
    private final NodeId id;
    @NonNull
    private final Address address;

    public NodeEndpoint(String id, String host, int port) {
        this(new NodeId(id), new Address(host, port));
    }
}
