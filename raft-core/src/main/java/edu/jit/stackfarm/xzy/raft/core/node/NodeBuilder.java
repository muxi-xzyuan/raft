package edu.jit.stackfarm.xzy.raft.core.node;

import com.google.common.eventbus.EventBus;
import edu.jit.stackfarm.xzy.raft.core.log.FileLog;
import edu.jit.stackfarm.xzy.raft.core.log.Log;
import edu.jit.stackfarm.xzy.raft.core.log.MemoryLog;
import edu.jit.stackfarm.xzy.raft.core.rpc.Connector;
import edu.jit.stackfarm.xzy.raft.core.rpc.nio.NioConnector;
import edu.jit.stackfarm.xzy.raft.core.schedule.DefaultScheduler;
import edu.jit.stackfarm.xzy.raft.core.schedule.Scheduler;
import edu.jit.stackfarm.xzy.raft.core.support.SingleThreadTaskExecutor;
import edu.jit.stackfarm.xzy.raft.core.support.TaskExecutor;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Collection;
import java.util.Collections;

/**
 * Node builder.
 */
public class NodeBuilder {

    /**
     * Group.
     */
    private final NodeGroup group;

    /**
     * Self id.
     */
    private final NodeId selfId;

    /**
     * Event bus, INTERNAL.
     */
    private final EventBus eventBus;

    /**
     * Node configuration.
     */
    private NodeConfig config = new NodeConfig();

    /**
     * Starts as standby or not.
     */
    private boolean quiet = false;

    /**
     * Log.
     * If data directory specified, {@link FileLog} will be created.
     */
    private Log log = null;

    /**
     * Scheduler, INTERNAL.
     */
    private Scheduler scheduler = null;

    /**
     * Connector, component to communicate between nodes, INTERNAL.
     */
    private Connector connector = null;

    /**
     * Task executor for node, INTERNAL.
     */
    private TaskExecutor taskExecutor = null;

    /**
     * Task executor for group config change task, INTERNAL.
     */
    private TaskExecutor groupConfigChangeTaskExecutor = null;

    /**
     * Event loop group for worker.
     * If specified, reuse. otherwise create one.
     */
    private NioEventLoopGroup workerNioEventLoopGroup = null;

    public NodeBuilder(@NonNull NodeEndpoint endpoint) {
        this(Collections.singletonList(endpoint), endpoint.getId());
    }

    public NodeBuilder(@NonNull Collection<NodeEndpoint> endpoints, @NonNull NodeId selfId) {
        this.group = new NodeGroup(endpoints, selfId);
        this.selfId = selfId;
        this.eventBus = new EventBus(selfId.getValue());
    }

    /**
     * Create.
     *
     * @param selfId self id
     * @param group  group
     */
    @Deprecated
    public NodeBuilder(@NonNull NodeId selfId, @NonNull NodeGroup group) {
        this.selfId = selfId;
        this.group = group;
        this.eventBus = new EventBus(selfId.getValue());
    }

    /**
     * Set quiet.
     *
     * @param quiet quiet
     * @return this
     */
    public NodeBuilder setQuiet(boolean quiet) {
        this.quiet = quiet;
        return this;
    }

    /**
     * Set configuration.
     *
     * @param config config
     * @return this
     */
    public NodeBuilder setConfig(@NonNull NodeConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Set connector.
     *
     * @param connector connector
     * @return this
     */
    NodeBuilder setConnector(@NonNull Connector connector) {
        this.connector = connector;
        return this;
    }

    /**
     * Set event loop for worker.
     * If specified, it's caller's responsibility to close worker event loop.
     *
     * @param workerNioEventLoopGroup worker event loop
     * @return this
     */
    public NodeBuilder setWorkerNioEventLoopGroup(@NonNull NioEventLoopGroup workerNioEventLoopGroup) {
        this.workerNioEventLoopGroup = workerNioEventLoopGroup;
        return this;
    }

    /**
     * Set scheduler.
     *
     * @param scheduler scheduler
     * @return this
     */
    NodeBuilder setScheduler(@NonNull Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Set task executor.
     *
     * @param taskExecutor task executor
     * @return this
     */
    NodeBuilder setTaskExecutor(@NonNull TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
        return this;
    }

    /**
     * Set group config change task executor.
     *
     * @param groupConfigChangeTaskExecutor group config change task executor
     * @return this
     */
    NodeBuilder setGroupConfigChangeTaskExecutor(@NonNull TaskExecutor groupConfigChangeTaskExecutor) {
        this.groupConfigChangeTaskExecutor = groupConfigChangeTaskExecutor;
        return this;
    }

    /**
     * Set data directory.
     *
     * @param dataDirPath data directory
     * @return this
     */
    public NodeBuilder setDataDir(@Nullable String dataDirPath) {
        if (dataDirPath == null || dataDirPath.isEmpty()) {
            return this;
        }
        File dataDir = new File(dataDirPath);
        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new IllegalArgumentException("[" + dataDirPath + "] not a directory, or not exists");
        }
        log = new FileLog(dataDir, eventBus, group.getEndpoints());
        return this;
    }

    /**
     * Build node.
     *
     * @return node
     */
    @Nonnull
    public Node build() {
        return new NodeImpl(buildContext());
    }

    /**
     * Build context for node.
     *
     * @return node context
     */
    @Nonnull
    private NodeContext buildContext() {
        NodeContext context = new NodeContext();
        context.setGroup(group);
        context.setMode(evaluateMode());
        context.setLog(log != null ? log : new MemoryLog(eventBus, group.getEndpoints()));
        context.setSelfId(selfId);
        context.setConfig(config);
        context.setEventBus(eventBus);
        context.setScheduler(scheduler != null ? scheduler : new DefaultScheduler(config));
        context.setConnector(connector != null ? connector : createNioConnector());
        context.setTaskExecutor(taskExecutor != null ? taskExecutor : new SingleThreadTaskExecutor("node"));
        context.setGroupConfigChangeTaskExecutor(groupConfigChangeTaskExecutor != null ? groupConfigChangeTaskExecutor :
                new SingleThreadTaskExecutor("group-config-change"));
        return context;
    }

    /**
     * Create nio connector.
     *
     * @return nio connector
     */
    @Nonnull
    private NioConnector createNioConnector() {
        int port = group.getSelf().getEndpoint().getAddress().getPort();
        if (workerNioEventLoopGroup != null) {
            return new NioConnector(workerNioEventLoopGroup, selfId, eventBus, port);
        }
        return new NioConnector(new NioEventLoopGroup(config.getNioWorkerThreads()), false, selfId, eventBus, port);
    }

    /**
     * Evaluate mode.
     *
     * @return mode
     * @see NodeGroup#isStandalone()
     */
    @Nonnull
    private NodeMode evaluateMode() {
        if (quiet) {
            return NodeMode.QUIET;
        }
        return NodeMode.STANDARD;
    }

}
