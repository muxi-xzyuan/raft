package edu.jit.stackfarm.xzy.raft.kvstore.message;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Success {

    public static final Success INSTANCE = new Success();

}
