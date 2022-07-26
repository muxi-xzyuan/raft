package edu.jit.stackfarm.xzy.raft.kvstore.client;

public interface Command {

    String getName();

    void execute(String arguments, CommandContext context);

}