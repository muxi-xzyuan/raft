package edu.jit.stackfarm.xzy.raft.kvstore.message;

import lombok.Data;

@Data
public class Failure {

    private final int errorCode;
    private final String message;

}
