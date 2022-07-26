package edu.jit.stackfarm.xzy.raft.core.node;

import edu.jit.stackfarm.xzy.raft.core.node.role.CandidateNodeRoleWrapper;
import edu.jit.stackfarm.xzy.raft.core.node.role.FollowerNodeRoleWrapper;
import edu.jit.stackfarm.xzy.raft.core.node.role.LeaderNodeRoleWrapper;
import edu.jit.stackfarm.xzy.raft.core.rpc.MockConnector;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.*;
import edu.jit.stackfarm.xzy.raft.core.schedule.NoobScheduler;
import edu.jit.stackfarm.xzy.raft.core.support.DirectTaskExecutor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class NodeImplTest {

    private NodeBuilder newNodeBuilder(NodeId selfId, NodeEndpoint... endpoints) {
        return new NodeBuilder(Arrays.asList(endpoints), selfId)
                .setScheduler(new NoobScheduler())
                .setConnector(new MockConnector())
                .setTaskExecutor(new DirectTaskExecutor());
    }

    @Test
    public void testStart() {
        NodeImpl node = (NodeImpl) newNodeBuilder(NodeId.of("A"), new NodeEndpoint("A", "localhost", 2333)).build();

        node.start();
        //启动后初始角色应为follower
        assertTrue(node.getRoleWrapper() instanceof FollowerNodeRoleWrapper);
        FollowerNodeRoleWrapper role = (FollowerNodeRoleWrapper) node.getRoleWrapper();
        //启动后初始任期为0
        assertEquals(0, role.getTerm());
        //启动后此节点无投票记录
        assertNull(role.getVotedFor());
    }

    @Test
    public void testElectionTimeoutWhenFollower() {
        NodeImpl node = (NodeImpl) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();

        node.start();
        node.electionTimeout();
        //选举超时后角色应为candidate
        assertTrue(node.getRoleWrapper() instanceof CandidateNodeRoleWrapper);
        CandidateNodeRoleWrapper role = (CandidateNodeRoleWrapper) node.getRoleWrapper();
        assertEquals(1, role.getTerm());
        assertEquals(1, role.getVotesCount());
        MockConnector mockConnector = (MockConnector) node.getContext().getConnector();
        RequestVoteRpc rpc = (RequestVoteRpc) mockConnector.getRpc();
        assertEquals(1, rpc.getTerm());
        assertEquals(NodeId.of("A"), rpc.getCandidateId());
        assertEquals(0, rpc.getLastLogIndex());
        assertEquals(0, rpc.getLastLogTerm());
    }

    @Test
    public void testOnReceiveRequestVoteRpcFollower() {
        NodeImpl node = (NodeImpl) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(1);
        rpc.setCandidateId(NodeId.of("C"));
        rpc.setLastLogIndex(0);
        rpc.setLastLogTerm(0);
        node.onReceiveRequestVoteRpc(new RequestVoteRpcMessage(rpc, NodeId.of("C"), null));
        MockConnector mockConnector = (MockConnector) node.getContext().getConnector();
        RequestVoteResult result = (RequestVoteResult) mockConnector.getResult();
        assertEquals(1, result.getTerm());
        assertTrue(result.isVoteGranted());
        assertEquals(NodeId.of("C"), ((FollowerNodeRoleWrapper) (node.getRoleWrapper())).getVotedFor());
    }

    @Test
    public void testOnReceiveRequestVoteResult() {
        NodeImpl node = (NodeImpl) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        node.electionTimeout();
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        LeaderNodeRoleWrapper role = (LeaderNodeRoleWrapper) node.getRoleWrapper();
        assertEquals(1, role.getTerm());
    }

    @Test
    public void testReplicateLog() {
        NodeImpl node = (NodeImpl) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        //发送RequestVote消息
        node.electionTimeout();
        node.onReceivePreVoteResult(new PreVoteResult(0, true));
        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
        //发送两条日志复制消息
        node.replicateLog();
        MockConnector mockConnector = (MockConnector) node.getContext().getConnector();
        //总共发送四条消息：PreVoteRpc, RequestVoteRpc, AppendEntriesRpc * 2
        assertEquals(4, mockConnector.getMessageCount());
        List<MockConnector.Message> messages = mockConnector.getMessages();
        Set<NodeId> destinationNodeIds = messages.subList(2, 4).stream()
                .map(MockConnector.Message::getDestinationNodeId)
                .collect(Collectors.toSet());
        assertEquals(2, destinationNodeIds.size());
        assertTrue(destinationNodeIds.contains(NodeId.of("B")));
        assertTrue(destinationNodeIds.contains(NodeId.of("C")));
        AppendEntriesRpc rpc = (AppendEntriesRpc) messages.get(2).getRpc();
        assertEquals(1, rpc.getTerm());
    }

    @Test
    public void testOnReceiveAppendEntriesRpcFollower() {
        NodeImpl node = (NodeImpl) newNodeBuilder(
                NodeId.of("A"),
                new NodeEndpoint("A", "localhost", 2333),
                new NodeEndpoint("B", "localhost", 2334),
                new NodeEndpoint("C", "localhost", 2335)
        ).build();
        node.start();
        AppendEntriesRpc rpc = new AppendEntriesRpc();
        rpc.setTerm(1);
        rpc.setLeaderId(NodeId.of("B"));
        node.onReceiveAppendEntriesRpc(new AppendEntriesRpcMessage(rpc, NodeId.of("B"), null));
        MockConnector connector = (MockConnector) node.getContext().getConnector();
        AppendEntriesResult result = (AppendEntriesResult) connector.getResult();
        assertEquals(1, result.getTerm());
        assertTrue(result.isSuccess());
        FollowerNodeRoleWrapper role = (FollowerNodeRoleWrapper) node.getRoleWrapper();
        assertEquals(1, role.getTerm());
        assertEquals(NodeId.of("B"), role.getLeaderId());
    }

//    @Test
//    public void testOnReceiveAppendEntriesNormal() {
//        NodeImpl node = (NodeImpl) newNodeBuilder(
//                NodeId.of("A"),
//                new NodeEndpoint("A", "localhost", 2333),
//                new NodeEndpoint("B", "localhost", 2334),
//                new NodeEndpoint("C", "localhost", 2335)
//        ).build();
//        node.start();
//        node.electionTimeout();
//        node.onReceiveRequestVoteResult(new RequestVoteResult(1, true));
//        node.replicateLog();
//        node.onReceivedAppendEntriesResult(new AppendEntriesResultMessage(
//                new AppendEntriesResult(1, true),
//                NodeId.of("B"),
//                new AppendEntriesRpc()
//        ));
//    }
}