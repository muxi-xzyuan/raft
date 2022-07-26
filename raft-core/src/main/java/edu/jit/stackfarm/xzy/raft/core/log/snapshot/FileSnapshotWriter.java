package edu.jit.stackfarm.xzy.raft.core.log.snapshot;


import edu.jit.stackfarm.xzy.raft.core.Protos;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Getter;

import java.io.*;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSnapshotWriter implements AutoCloseable {

    @Getter
    private final DataOutputStream output;

    public FileSnapshotWriter(File file, int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> lastConfig) throws IOException {
        this(new DataOutputStream(new FileOutputStream(file)), lastIncludedIndex, lastIncludedTerm, lastConfig);
    }

    FileSnapshotWriter(OutputStream output, int lastIncludedIndex, int lastIncludedTerm, Set<NodeEndpoint> lastConfig) throws IOException {
        this.output = new DataOutputStream(output);
        byte[] headerBytes = Protos.SnapshotHeader.newBuilder()
                .setLastIndex(lastIncludedIndex)
                .setLastTerm(lastIncludedTerm)
                .addAllLastConfig(
                        lastConfig.stream()
                                .map(e -> Protos.NodeEndpoint.newBuilder()
                                        .setId(e.getId().getValue())
                                        .setHost(e.getAddress().getHost())
                                        .setPort(e.getAddress().getPort())
                                        .build())
                                .collect(Collectors.toList()))
                .build().toByteArray();
        this.output.writeInt(headerBytes.length);
        this.output.write(headerBytes);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    public void write(byte[] data) throws IOException {
        output.write(data);
    }

}