package edu.jit.stackfarm.xzy.raft.core.node;

import com.google.common.base.Preconditions;
import edu.jit.stackfarm.xzy.raft.core.node.role.NodeRole;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Thrown when current node is not leader.
 */
public class NotLeaderException extends RuntimeException {

    private final NodeRole nodeRole;
    private final NodeEndpoint leaderEndpoint;

    /**
     * Create.
     *
     * @param nodeRole       role name
     * @param leaderEndpoint leader endpoint
     */
    public NotLeaderException(@Nonnull NodeRole nodeRole, @Nullable NodeEndpoint leaderEndpoint) {
        super("not leader");
        Preconditions.checkNotNull(nodeRole);
        this.nodeRole = nodeRole;
        this.leaderEndpoint = leaderEndpoint;
    }

    /**
     * Get role name.
     *
     * @return role name
     */
    @Nonnull
    public NodeRole getNodeRole() {
        return nodeRole;
    }

    /**
     * Get leader endpoint.
     *
     * @return leader endpoint
     */
    @Nullable
    public NodeEndpoint getLeaderEndpoint() {
        return leaderEndpoint;
    }

}
