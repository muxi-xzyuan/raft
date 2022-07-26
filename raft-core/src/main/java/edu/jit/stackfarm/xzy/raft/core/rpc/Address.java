package edu.jit.stackfarm.xzy.raft.core.rpc;

import lombok.Data;
import lombok.NonNull;

import javax.annotation.concurrent.Immutable;

@Data
@Immutable
public class Address {

    @NonNull
    private final String host;
    private final int port;

}
