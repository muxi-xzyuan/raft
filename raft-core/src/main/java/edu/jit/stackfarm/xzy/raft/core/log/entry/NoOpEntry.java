package edu.jit.stackfarm.xzy.raft.core.log.entry;

import lombok.ToString;

@ToString
public class NoOpEntry extends AbstractEntry {

    public NoOpEntry(int index, int term) {
        super(Entry.KIND_NO_OP, index, term);
    }

    @Override
    public byte[] getCommandBytes() {
        return new byte[0];
    }
}
