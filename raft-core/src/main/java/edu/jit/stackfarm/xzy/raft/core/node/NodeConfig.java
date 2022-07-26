package edu.jit.stackfarm.xzy.raft.core.node;

import edu.jit.stackfarm.xzy.raft.core.log.Log;
import lombok.Data;

@Data
public class NodeConfig {

    /**
     * Minimum time of election timeout.
     */
    private int minElectionTimeout = 3000;

    /**
     * Maximum time of election timeout.
     */
    private int maxElectionTimeout = 4000;

    /**
     * The delay of first log replication after becoming leader.
     */
    private int logReplicationDelay = 0;

    /**
     * Interval for log replication task. More specifically, interval for heartbeat rpc.
     */
    private int logReplicationInterval = 1000;

    /**
     * Worker thread count in nio connector.
     * Zero means twice the number of available processors.
     */
    private int nioWorkerThreads = 0;

    /**
     * Max number of entries sent when replicate log to followers.
     */
    private int maxReplicationEntries = Log.ALL_ENTRIES;

    /**
     * Max entries to send when replicate log to new node
     */
    private int maxReplicationEntriesForNewNode = Log.ALL_ENTRIES;

    /**
     * Data length in install snapshot rpc.
     */
    private int snapshotDataLength = 1024;

    /**
     * Read timeout to receive response from follower.
     * If no response received from follower, resend log replication rpc.
     */
    private int logReplicationReadTimeout = 900;

    /**
     * Max round for new node to catch up.
     */
    private int newNodeMaxRound = 10;

    /**
     * Read timeout to receive response from new node.
     * Default to election timeout.
     */
    private int newNodeReadTimeout = 3000;

    /**
     * Timeout for new node to make progress.<br>
     * If new node cannot make progress before this timeout, new node cannot be added and reply TIMEOUT.<br>
     * Default to election timeout.
     */
    private int newNodeAdvanceTimeout = 3000;

    /**
     * Timeout to wait for previous group config change to complete.
     * Default is {@code 0}, forever.
     */
    private int previousGroupConfigChangeTimeout = 0;
}
