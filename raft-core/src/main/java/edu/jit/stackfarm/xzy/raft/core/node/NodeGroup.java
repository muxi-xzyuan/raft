package edu.jit.stackfarm.xzy.raft.core.node;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class NodeGroup {

    private final NodeId selfId;
    private Map<NodeId, GroupMember> memberMap;

    NodeGroup(Collection<NodeEndpoint> endpoints, NodeId selfId) {
        memberMap = buildMemberMap(endpoints);
        this.selfId = selfId;
    }

    /**
     * Returns the number of nodes in this cluster, used in election.
     *
     * @return the number of nodes in this cluster
     */
    int getNodesCount() {
        return memberMap.values().size();
    }

    /**
     * Find member by id.
     * <p>Throw exception if member not found.</p>
     *
     * @param nodeId node id
     * @return member, never be {@code null}
     * @throws IllegalArgumentException if member not found
     */
    @Nonnull
    GroupMember findMember(NodeId nodeId) {
        GroupMember member = getMember(nodeId);
        if (member == null) {
            throw new IllegalArgumentException("no such node " + nodeId);
        }
        return member;
    }

    /**
     * Get member by id.
     *
     * @param nodeId node id
     * @return member, maybe {@code null}
     */
    @Nullable
    GroupMember getMember(NodeId nodeId) {
        return memberMap.get(nodeId);
    }

    Collection<GroupMember> getReplicationTargets() {
        return memberMap.values().stream().filter(m -> !m.idEquals(selfId)).collect(Collectors.toList());
    }

    Set<NodeEndpoint> getEndpointsExceptSelf() {
        return memberMap.values().stream()
                .filter((member) -> !member.idEquals(selfId))
                .map(GroupMember::getEndpoint)
                .collect(Collectors.toSet());
    }

    Set<NodeId> getNodeIdsExceptSelf() {
        return memberMap.values().stream()
                .filter((member) -> !member.idEquals(selfId))
                .map((member) -> member.getEndpoint().getId())
                .collect(Collectors.toSet());
    }

    boolean isStandalone() {
        return memberMap.size() == 1 && memberMap.containsKey(selfId);
    }

    GroupMember getSelf() {
        return findMember(selfId);
    }

    /**
     * 计算过半MatchIndex，作为推进commitIndex时新的commitIndex
     */
    int getNewCommitIndex() {
        //计算过程如下：收集除了自己以外节点的matchIndex，从小到大排序并取中间位置的matchIndex为过半matchIndex
        //虽然这个计算方式本身是存在问题的（当leader掉线时结果不正确），但由于这个函数只能被leader调用，所以leader掉线导致此函数结果不正确的情况并不会发生
        List<Integer> matchIndexes = memberMap.values().stream()
                //排除非major节点和自己
                .filter((member) -> !member.idEquals(selfId))
                .map((member) -> member.getReplicatingState().getMatchIndex())
                //从小到大排序
                .sorted()
                .collect(Collectors.toList());
        int count = matchIndexes.size();
        if (count == 0) {
            throw new IllegalStateException("Standalone or no major node");
        }
        return matchIndexes.get(count / 2);
    }

    /**
     * 重置其他所有节点复制进度，在此节点成为Leader后调用
     *
     * @param nextLogIndex 此节点的nextLogIndex
     */
    void resetReplicatingStates(int nextLogIndex) {
        for (GroupMember member : memberMap.values()) {
            if (!member.idEquals(selfId)) {
                member.setReplicatingState(new ReplicatingState(nextLogIndex));
            }
        }
    }

    void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
        log.info("Add node {} to group", endpoint.getId());
        ReplicatingState replicatingState = new ReplicatingState(nextIndex, matchIndex);
        GroupMember member = new GroupMember(endpoint, replicatingState);
        memberMap.put(endpoint.getId(), member);
    }

    void removeNode(NodeId id) {
        log.info("Node {} removed", id);
        memberMap.remove(id);
    }

    /**
     * Update member list.
     * <p>All replicating state will be dropped.</p>
     *
     * @param endpoints endpoints
     */
    void updateNodes(Set<NodeEndpoint> endpoints) {
        memberMap = buildMemberMap(endpoints);
        log.info("group config changed -> {}", memberMap.keySet());
    }

    Set<NodeEndpoint> getEndpoints() {
        Set<NodeEndpoint> endpoints = new HashSet<>();
        for (GroupMember member : memberMap.values()) {
            endpoints.add(member.getEndpoint());
        }
        return endpoints;
    }

    private Map<NodeId, GroupMember> buildMemberMap(Collection<NodeEndpoint> endpoints) {
        Map<NodeId, GroupMember> map = new HashMap<>();
        endpoints.forEach(endpoint -> map.put(endpoint.getId(), new GroupMember(endpoint)));
        if (map.isEmpty()) {
            throw new IllegalArgumentException("endpoints is empty");
        }
        return map;
    }

}
