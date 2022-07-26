package edu.jit.stackfarm.xzy.raft.core.node;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.log.Log;
import edu.jit.stackfarm.xzy.raft.core.rpc.Connector;
import edu.jit.stackfarm.xzy.raft.core.schedule.Scheduler;
import edu.jit.stackfarm.xzy.raft.core.support.TaskExecutor;
import lombok.Data;

/**
 * 持有其他组件的引用，避免核心组件直接持有各个组件的引用
 */
@Data
public class NodeContext {

    private NodeConfig config;
    private NodeId selfId;
    private NodeGroup group;
    private Connector connector;
    private Scheduler scheduler;
    private EventBus eventBus;

    /**
     * Main thread task executor, used to switch thread from IO threads to main thread.
     */
    private TaskExecutor taskExecutor;

    /**
     * The task executor to run group config change tasks.
     */
    private TaskExecutor groupConfigChangeTaskExecutor;
    private Log log;
    private NodeMode mode;
}
