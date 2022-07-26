package edu.jit.stackfarm.xzy.raft.core.rpc.message;


import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Data;

import java.util.Set;

/**
 * InstallSnapshot被设计为允许分次传输的消息，增加了数据偏移offset和判断是否全部传输完毕的done字段。
 * 具体每次传输多少数据为好，一般根据网络环境、数据大小来决定，建议做成可配置的参数
 */
@Data
public class InstallSnapshotRpc {

    private int term;
    private NodeId leaderId;
    private int lastIndex;
    private int lastTerm;
    private Set<NodeEndpoint> lastConfig;
    private int offset;
    private byte[] data;
    private boolean done;

    public int getDataLength() {
        return this.data.length;
    }

    @Override
    public String toString() {
        return "InstallSnapshotRpc{" +
                "data.length=" + (data != null ? data.length : 0) +
                ", done=" + done +
                ", lastIndex=" + lastIndex +
                ", lastTerm=" + lastTerm +
                ", leaderId=" + leaderId +
                ", offset=" + offset +
                ", term=" + term +
                '}';
    }

}
