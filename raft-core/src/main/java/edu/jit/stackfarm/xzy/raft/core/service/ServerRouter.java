package edu.jit.stackfarm.xzy.raft.core.service;

import com.google.common.util.concurrent.FutureCallback;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class ServerRouter {

    private final Map<NodeId, Channel> availableServers = new HashMap<>();
    private final List<NodeId> tried = new ArrayList<>();
    @Getter
    private NodeId leaderId;

    public Object send(FutureCallback<Object> command) {
        Object result;
        for (NodeId nodeId : getCandidateNodeIds()) {
            try {
                result = doSend(nodeId, command);
                // success
                command.onSuccess(result);
                this.leaderId = nodeId;
                tried.clear();
                return result;
            } catch (RedirectException e) {
                if (tried.contains(e.getLeaderId())) {
                    log.debug("Server {} is not a leader, but the server {} to be redirected to has been tried, skip.", nodeId, e.getLeaderId());
                    continue;
                }
                //收到重定向请求，修改Leader节点id
                log.debug("Server {} is not a leader, redirect to server {}", nodeId, e.getLeaderId());
                this.leaderId = e.getLeaderId();
                try {
                    result = doSend(e.getLeaderId(), command);
                    // success
                    command.onSuccess(result);
                    tried.clear();
                    return result;
                } catch (Exception exception) {
                    log.debug("Failed to process with server " + e.getLeaderId() + ", cause " + exception.getMessage());
                }
            } catch (Exception e) {
                //连接失败，尝试下一个节点
                log.debug("Failed to process with server " + nodeId + ", cause " + e.getMessage());
            }
        }
        tried.clear();
        command.onFailure(null);
        return null;
    }

    public void add(NodeId id, Channel channel) {
        this.availableServers.put(id, channel);
    }

    public void setLeaderId(NodeId leaderId) {
        if (!availableServers.containsKey(leaderId)) {
            throw new IllegalStateException("No such server [" + leaderId + "] in list");
        }
        this.leaderId = leaderId;
    }

    //获取候选节点id列表
    private Collection<NodeId> getCandidateNodeIds() {
        //候选为空
        if (availableServers.isEmpty()) {
            throw new NoAvailableServerException("No available server");
        }
        //已设置
        if (leaderId != null) {
            List<NodeId> nodeIds = new ArrayList<>();
            nodeIds.add(leaderId);
            for (NodeId nodeId : availableServers.keySet()) {
                if (!nodeId.equals(leaderId)) {
                    nodeIds.add(nodeId);
                }
            }
            return nodeIds;
        }
        //没有设置的话，任意返回
        return availableServers.keySet();
    }

    private Object doSend(NodeId nodeId, Object payload) {
        tried.add(nodeId);
        Channel channel = this.availableServers.get(nodeId);
        if (channel == null) {
            throw new IllegalStateException("No such channel to server " + nodeId);
        }
        log.debug("Send request to server {}", nodeId);
        return channel.send(payload);
    }
}
