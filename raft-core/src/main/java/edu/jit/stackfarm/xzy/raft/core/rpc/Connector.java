package edu.jit.stackfarm.xzy.raft.core.rpc;

import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.AbstractRpcMessage;
import lombok.NonNull;

import java.util.Collection;

/**
 * 发送RPC请求与响应RPC请求的接口
 */
public interface Connector {

    /**
     * 初始化
     */
    void initialize();

    /**
     * Send rpc to a single node.
     *
     * @param rpc            rpc
     * @param targetEndpoint target endpoint
     */
    void send(@NonNull Object rpc, @NonNull NodeEndpoint targetEndpoint);

    /**
     * Send rpc to many nodes.
     *
     * @param rpc             rpc
     * @param targetEndpoints target endpoints
     */
    void send(@NonNull Object rpc, @NonNull Collection<NodeEndpoint> targetEndpoints);

    /**
     * Reply result.
     *
     * @param result     result
     * @param rpcMessage rpc, source node id and the channel to reply
     */
    void reply(@NonNull Object result, @NonNull AbstractRpcMessage<?> rpcMessage);

    void close();

}
