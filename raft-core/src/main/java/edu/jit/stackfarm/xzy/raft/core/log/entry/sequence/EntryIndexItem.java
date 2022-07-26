package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryMetaData;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.concurrent.Immutable;

@Immutable
@Getter(AccessLevel.PACKAGE)
@AllArgsConstructor(access = AccessLevel.PACKAGE)
class EntryIndexItem {

    private final int index;
    private final long offset;
    private final int kind;
    private final int term;

    EntryMetaData toEntryMetaData() {
        return new EntryMetaData(kind, index, term);
    }
}
