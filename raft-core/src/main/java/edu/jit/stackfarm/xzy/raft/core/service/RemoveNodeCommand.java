package edu.jit.stackfarm.xzy.raft.core.service;


import com.google.common.util.concurrent.FutureCallback;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class RemoveNodeCommand implements FutureCallback<Object> {

    private final NodeId nodeId;

    @Override
    public void onSuccess(Object result) {
        log.info("Remove node command succeeded.");
    }

    @Override
    public void onFailure(Throwable t) {
        log.info("Remove node command failed.");
    }

}
