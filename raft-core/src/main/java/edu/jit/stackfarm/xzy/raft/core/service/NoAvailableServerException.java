package edu.jit.stackfarm.xzy.raft.core.service;

public class NoAvailableServerException extends RuntimeException {

    public NoAvailableServerException(String message) {
        super(message);
    }

}