package edu.jit.stackfarm.xzy.raft.core.log.entry;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
abstract class AbstractEntry implements Entry {

    private final int kind;
    private final int index;
    private final int term;

    @Override
    public EntryMetaData getMetaData() {
        return new EntryMetaData(kind, index, term);
    }

}
