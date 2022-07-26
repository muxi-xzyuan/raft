package edu.jit.stackfarm.xzy.raft.core.rpc.message;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class InstallSnapshotResult {

    private final int term;

}
