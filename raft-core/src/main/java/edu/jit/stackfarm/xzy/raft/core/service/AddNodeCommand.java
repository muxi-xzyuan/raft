package edu.jit.stackfarm.xzy.raft.core.service;

import com.google.common.util.concurrent.FutureCallback;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class AddNodeCommand implements FutureCallback<Object> {

    private final String nodeId;
    private final String host;
    private final int port;

    public NodeEndpoint toNodeEndpoint() {
        return new NodeEndpoint(nodeId, host, port);
    }

    @Override
    public void onSuccess(Object result) {
        log.info("Add node command succeeded.");
    }

    @Override
    public void onFailure(Throwable t) {
        log.info("Add node command failed.");
    }

}
