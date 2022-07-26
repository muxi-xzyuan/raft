package edu.jit.stackfarm.xzy.raft.kvstore.client;


public class RaftRemoveNodeCommand implements Command {

    @Override
    public String getName() {
        return "raft-remove-node";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        if (arguments.isEmpty()) {
            throw new IllegalArgumentException("Usage " + getName() + " <node-id>");
        }
        context.getClient().removeNode(arguments);
    }

}
