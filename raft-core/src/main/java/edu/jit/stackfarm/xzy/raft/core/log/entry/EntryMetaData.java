package edu.jit.stackfarm.xzy.raft.core.log.entry;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class EntryMetaData {

    private final int kind;
    private final int index;
    private final int term;

}
