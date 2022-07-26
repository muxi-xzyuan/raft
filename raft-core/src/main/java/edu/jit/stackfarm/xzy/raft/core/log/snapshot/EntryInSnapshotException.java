package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import edu.jit.stackfarm.xzy.raft.core.log.LogException;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class EntryInSnapshotException extends LogException {

    private final int index;
}
