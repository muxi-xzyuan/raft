package edu.jit.stackfarm.xzy.raft.core.log.entry;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class GeneralEntry extends AbstractEntry {

    private final byte[] commandBytes;

    public GeneralEntry(int index, int term, byte[] commandBytes) {
        super(Entry.KIND_GENERAL, index, term);
        this.commandBytes = commandBytes;
    }
}
