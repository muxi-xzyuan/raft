package edu.jit.stackfarm.xzy.raft.core.log.entry;


import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;

import java.util.Set;

public abstract class GroupConfigEntry extends AbstractEntry {

    private final Set<NodeEndpoint> nodeEndpoints;

    protected GroupConfigEntry(int kind, int index, int term, Set<NodeEndpoint> nodeEndpoints) {
        super(kind, index, term);
        this.nodeEndpoints = nodeEndpoints;
    }

    /**
     * 获得该集群配置日志条目生效之前的集群配置
     *
     * @return 该集群配置日志条目生效之前的集群配置
     */
    public Set<NodeEndpoint> getNodeEndpoints() {
        return nodeEndpoints;
    }

    /**
     * 获得该集群配置日志条目生效之后的集群配置
     *
     * @return 该集群配置日志条目生效之后的集群配置
     */
    public abstract Set<NodeEndpoint> getResultNodeEndpoints();

}
