package edu.jit.stackfarm.xzy.raft.kvstore.client;

public class RaftUpdateNodeCommand implements Command {

    @Override
    public String getName() {
        return "raft-update-node";
    }

    @Override
    public void execute(String arguments, CommandContext context) {
        // <node-id> <host> <port-raft-node>
        String[] pieces = arguments.split("\\s");
        if (pieces.length != 3) {
            throw new IllegalArgumentException("Usage " + getName() + " <node-id> <host> <raft-node-port>");
        }
        String nodeId = pieces[0];
        String host = pieces[1];
        int port;
        try {
            port = Integer.parseInt(pieces[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal port [" + pieces[2] + "]");
        }
        context.getClient().updateNode(nodeId, host, port);
    }
}
