package edu.jit.stackfarm.xzy.raft.core.log.event;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SnapshotGeneratedEvent {

    private final int lastIncludedIndex;

}
