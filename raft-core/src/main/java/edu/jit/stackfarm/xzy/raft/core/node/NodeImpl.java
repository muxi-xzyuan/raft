package edu.jit.stackfarm.xzy.raft.core.node;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.Subscribe;
import edu.jit.stackfarm.xzy.raft.core.log.AppendEntriesState;
import edu.jit.stackfarm.xzy.raft.core.log.entry.GroupConfigEntry;
import edu.jit.stackfarm.xzy.raft.core.log.event.SnapshotGeneratedEvent;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.EntryInSnapshotException;
import edu.jit.stackfarm.xzy.raft.core.log.snapshot.InstallSnapshotState;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.StateMachine;
import edu.jit.stackfarm.xzy.raft.core.node.role.*;
import edu.jit.stackfarm.xzy.raft.core.node.task.*;
import edu.jit.stackfarm.xzy.raft.core.rpc.message.*;
import edu.jit.stackfarm.xzy.raft.core.schedule.ElectionTimeout;
import edu.jit.stackfarm.xzy.raft.core.schedule.LogReplicationTask;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * 一致性核心组件
 */
@Slf4j
public class NodeImpl implements Node {

    private final NodeContext context;
    // NewNodeCatchUpTask and GroupConfigChangeTask related
    private final NewNodeCatchUpTaskContext newNodeCatchUpTaskContext = new NewNodeCatchUpTaskContextImpl();
    private final NewNodeCatchUpTaskGroup newNodeCatchUpTaskGroup = new NewNodeCatchUpTaskGroup();
    private final GroupConfigChangeTaskContext groupConfigChangeTaskContext = new GroupConfigChangeTaskContextImpl();
    private volatile GroupConfigChangeTask currentGroupConfigChangeTask = GroupConfigChangeTask.NO_TASK;
    private volatile GroupConfigChangeTaskReference currentGroupConfigChangeTaskReference
            = new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK);
    // Map<RequestId, ReadIndexTask>
    private final Map<String, ReadTask> readTaskMap = new HashMap<>();
    private boolean started;
    private AbstractNodeRoleWrapper roleWrapper;

    public NodeImpl(NodeContext context) {
        this.context = context;
    }

    @Override
    public synchronized void start() {
        //如果已经启动，则直接跳过
        if (started) {
            return;
        }
        //将自己注册到EventBus
        context.getEventBus().register(this);
        //初始化连接器
        context.getConnector().initialize();
        Set<NodeEndpoint> latestGroupConfig = context.getLog().getLatestGroupConfig();
        if (!latestGroupConfig.isEmpty()) {
            context.getGroup().updateNodes(latestGroupConfig);
        }
        changeRoleTo(new FollowerNodeRoleWrapper(context.getLog().getLastLogTerm(), null, null, 0, scheduleElectionTimeout()));
        started = true;
    }

    @Nonnull
    @Override
    public NodeRole getNodeRole() {
        return roleWrapper.getRole();
    }

    @Override
    public NodeId getLeaderId() {
        return roleWrapper.getLeaderId(context.getSelfId());
    }

    @Override
    public void enqueueReadIndex(@NonNull String requestId) {
        context.getTaskExecutor().execute(() -> {
            ReadTask task = new ReadTask(
                    context.getLog().getCommitIndex(),
                    context.getGroup().getNodeIdsExceptSelf(),
                    context.getLog().getStateMachine(),
                    requestId
            );
            log.debug("Enqueue read index task {}", task);
            readTaskMap.put(requestId, task);
            doReplicateLog();
        });
    }

    @Override
    public void appendLog(byte[] commandBytes) {
        Preconditions.checkNotNull(commandBytes);
        ensureLeader();
        context.getTaskExecutor().execute(() -> {
            //当街节点追加日志条目
            context.getLog().appendEntry(roleWrapper.getTerm(), commandBytes);
            //将日志复制到其他节点
            doReplicateLog();
        });
    }

    @Override
    public void registerStateMachine(StateMachine stateMachine) {
        context.getLog().setStateMachine(stateMachine);
    }

    @Nonnull
    @Override
    public GroupConfigChangeTaskReference addNode(@NonNull NodeEndpoint endpoint) throws NodeAlreadyAddedException {
        ensureLeader();
        if (context.getGroup().getMember(endpoint.getId()) != null) {
            throw new NodeAlreadyAddedException(endpoint.getId());
        }
        // self cannot be added
        if (context.getSelfId().equals(endpoint.getId())) {
            throw new IllegalArgumentException("New node cannot be self.");
        }
        NewNodeCatchUpTask newNodeCatchUpTask = new NewNodeCatchUpTask(newNodeCatchUpTaskContext, endpoint, context.getConfig());
        // task for node exists
        if (!newNodeCatchUpTaskGroup.add(newNodeCatchUpTask)) {
            throw new IllegalArgumentException("Node " + endpoint.getId() + " is catching up.");
        }
        // catch up new server, run in caller thread
        NewNodeCatchUpTaskResult newNodeCatchUpTaskResult;
        try {
            newNodeCatchUpTaskResult = newNodeCatchUpTask.call();
            switch (newNodeCatchUpTaskResult.getState()) {
                case REPLICATION_FAILED:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.REPLICATION_FAILED);
                case TIMEOUT:
                    return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.TIMEOUT);
            }
        } catch (Exception e) {
            if (!(e instanceof InterruptedException)) {
                log.warn("New node failed to catch up " + endpoint.getId(), e);
            }
            return new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.ERROR);
        }
        // new server caught up
        // wait for previous group config change
        // it will wait forever by default, but you can change to fixed timeout by setting in NodeConfig
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }
        // submit group config change task
        synchronized (this) {
            // it will happen when try to add two or more nodes at the same time
            if (currentGroupConfigChangeTask != GroupConfigChangeTask.NO_TASK) {
                throw new IllegalStateException("Group config change concurrently");
            }
            currentGroupConfigChangeTask = new AddNodeTask(groupConfigChangeTaskContext, endpoint, newNodeCatchUpTaskResult);
            Future<GroupConfigChangeTaskResult> future = context.getGroupConfigChangeTaskExecutor().submit(currentGroupConfigChangeTask);
            currentGroupConfigChangeTaskReference = new FutureGroupConfigChangeTaskReference(future);
            return currentGroupConfigChangeTaskReference;
        }
    }

    @Nonnull
    @Override
    public GroupConfigChangeTaskReference removeNode(@NonNull NodeId nodeId) {
        ensureLeader();
        // await previous group config change task
        GroupConfigChangeTaskResult result = awaitPreviousGroupConfigChangeTask();
        if (result != null) {
            return new FixedResultGroupConfigTaskReference(result);
        }
        // submit group config change task
        synchronized (this) {
            // it will happen when try to remove two or more nodes at the same time
            if (currentGroupConfigChangeTask != GroupConfigChangeTask.NO_TASK) {
                throw new IllegalStateException("Group config change concurrently");
            }

            currentGroupConfigChangeTask = new RemoveNodeTask(groupConfigChangeTaskContext, nodeId);
            Future<GroupConfigChangeTaskResult> future = context.getGroupConfigChangeTaskExecutor().submit(currentGroupConfigChangeTask);
            currentGroupConfigChangeTaskReference = new FutureGroupConfigChangeTaskReference(future);
            return currentGroupConfigChangeTaskReference;
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("node is not started, can not stop it!");
        }
        context.getScheduler().stop();
        context.getLog().close();
        context.getConnector().close();
        context.getTaskExecutor().shutdown();
        context.getGroupConfigChangeTaskExecutor().shutdown();
        started = false;
    }

    //=============================   此节点收到预投票请求   =============================
    @Subscribe
    public void onReceivePreVoteRpc(PreVoteRpcMessage message) {
        context.getTaskExecutor().execute(
                () -> context.getConnector().reply(doProcessPreVoteRpc(message), message)
        );
    }

    private PreVoteResult doProcessPreVoteRpc(PreVoteRpcMessage rpcMessage) {
        PreVoteRpc rpc = rpcMessage.getRpc();
        return new PreVoteResult(roleWrapper.getTerm(), !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm()));
    }

    //=============================   此节点收到预投票的结果   =============================
    @Subscribe
    public void onReceivePreVoteResult(PreVoteResult result) {
        context.getTaskExecutor().execute(() -> doProcessPreVoteResult(result));
    }

    private void doProcessPreVoteResult(PreVoteResult result) {
        if (roleWrapper.getRole() != NodeRole.FOLLOWER) {
            log.warn("Receive pre vote result when current role is not follower, ignore.");
            return;
        }
        if (!result.isVoteGranted()) {
            return;
        }
        int currentPreVoteCount = ((FollowerNodeRoleWrapper) roleWrapper).getPreVotesCount() + 1;
        int nodesCount = context.getGroup().getNodesCount();
        log.debug("Pre-election votes count {}, nodes count {}", currentPreVoteCount, nodesCount);
        roleWrapper.cancelTimeoutOrTask();
        if (currentPreVoteCount > nodesCount / 2) {
            // Received a majority of votes in pre-election, start formal election.
            startElection(roleWrapper.getTerm() + 1);
        } else {
            becomeFollower(roleWrapper.getTerm(), null, null, currentPreVoteCount);
        }
    }

    //=============================   此节点收到正式投票请求   =============================
    @Subscribe
    public void onReceiveRequestVoteRpc(RequestVoteRpcMessage message) {
        context.getTaskExecutor().execute(
                () -> context.getConnector().reply(doProcessRequestVoteRpc(message), message)
        );
    }

    private RequestVoteResult doProcessRequestVoteRpc(RequestVoteRpcMessage message) {
        RequestVoteRpc rpc = message.getRpc();
        //对方任期比己方小，处于落后状态，不投票并返回己方的term
        if (rpc.getTerm() < roleWrapper.getTerm()) {
            log.debug("Term from rpc({}) < current term({}), don't vote", rpc.getTerm(), roleWrapper.getTerm());
            return new RequestVoteResult(roleWrapper.getTerm(), false);
        }

        //本地日志比对方日志新则不投票
        boolean voteGranted = !context.getLog().isNewerThan(rpc.getLastLogIndex(), rpc.getLastLogTerm());

        //对方任期比己方大，则变为follower
        if (rpc.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive request vote rpc, term in rpc > my term");
            becomeFollower(rpc.getTerm(), (voteGranted ? rpc.getCandidateId() : null), null);
            return new RequestVoteResult(rpc.getTerm(), voteGranted);
        }
        assert rpc.getTerm() == roleWrapper.getTerm();
        //对方任期与己方一致
        switch (roleWrapper.getRole()) {
            case FOLLOWER:
                FollowerNodeRoleWrapper followerNodeRole = (FollowerNodeRoleWrapper) roleWrapper;
                NodeId votedFor = followerNodeRole.getVotedFor();
                //以下两种情况投票
                //case 1. 自己尚未投过票，并且对方的日志比自己新
                //case 2. 自己已经给对方投过票
                //投票后需要切换为follower角色
                if ((votedFor == null && voteGranted) || //case 1
                        Objects.equals(votedFor, rpc.getCandidateId())) { //case 2
                    becomeFollower(roleWrapper.getTerm(), rpc.getCandidateId(), null);
                    return new RequestVoteResult(rpc.getTerm(), true);
                }
            case CANDIDATE: //已经给自己投过票，所以不会给其他节点投票
            case LEADER:
                return new RequestVoteResult(roleWrapper.getTerm(), false);
            default:
                throw new IllegalStateException("Unexpected node role [" + roleWrapper.getRole() + "]");
        }

    }

    //=============================   此节点收到正式投票的结果   =============================
    @Subscribe
    public void onReceiveRequestVoteResult(RequestVoteResult result) {
        context.getTaskExecutor().execute(
                () -> doProcessRequestVoteResult(result)
        );
    }

    private void doProcessRequestVoteResult(RequestVoteResult result) {
        //对方term比己方大则己方变为follower
        if (result.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive request vote result, term in result > my term");
            becomeFollower(result.getTerm(), null, null);
            return;
        }
        // check role
        if (roleWrapper.getRole() != NodeRole.CANDIDATE) {
            log.debug("Receive request vote result and current role is not candidate, ignore");
            return;
        }
        //以下两种情况直接无视结果
        //case 1. 对方任期比己方小
        //case 2. 对方未给己方投票
        if (result.getTerm() < roleWrapper.getTerm() || //case 1
                !result.isVoteGranted()) { //case 2
            return;
        }
        //取消选举定时器，因为是相同term收到请求投票结果，所以己方只能是candidate，所以此方法必然是取消选举超时定时器？
        //错，相同term收到投票结果，己方也可能是leader，因为之前是candidate，收到过半票之后变为leader，此时仍会收到剩余票
        //所以只应该取消选举超时，不能取消日志复制，需要进行角色判断
        //角色判断放在上面了
        roleWrapper.cancelTimeoutOrTask();
        //票数
        int currentVoteCount = ((CandidateNodeRoleWrapper) roleWrapper).getVotesCount() + 1;
        //major节点数
        int count = context.getGroup().getNodesCount();
        log.debug("Votes count {}, node count {}", currentVoteCount, count);

        if (currentVoteCount > count / 2) {
            //票数过半，成为leader
            log.info("Become leader, term {}", roleWrapper.getTerm());
            resetReplicatingStates();
            changeRoleTo(new LeaderNodeRoleWrapper(roleWrapper.getTerm(), scheduleLogReplicationTask()));
            context.getLog().appendEntry(roleWrapper.getTerm());
        } else {
            //变更票数，重创选举定时器
            changeRoleTo(new CandidateNodeRoleWrapper(roleWrapper.getTerm(), currentVoteCount, scheduleElectionTimeout()));
        }
    }

    //=============================   此节点收到追加日志请求   =============================
    @Subscribe
    public void onReceiveAppendEntriesRpc(AppendEntriesRpcMessage message) {
        context.getTaskExecutor().execute(
                () -> context.getConnector().reply(doProcessAppendEntriesRpc(message), message)
        );
    }

    private AppendEntriesResult doProcessAppendEntriesRpc(AppendEntriesRpcMessage message) {
        AppendEntriesRpc rpc = message.getRpc();
        //对方任期比己方小，告知对方己方term
        if (rpc.getTerm() < roleWrapper.getTerm()) {
            return new AppendEntriesResult(roleWrapper.getTerm(), false, rpc.getMessageId());
        }
        //对方term比己方大，己方退化为follower
        if (rpc.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive append entries rpc, term in rpc > my term");
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId());
            //并追加日志
            return new AppendEntriesResult(rpc.getTerm(), appendEntries(rpc), rpc.getMessageId());
        }
        assert rpc.getTerm() == roleWrapper.getTerm();
        switch (roleWrapper.getRole()) {
            case FOLLOWER:
                //设置leaderId并重置选举定时器
                becomeFollower(rpc.getTerm(), ((FollowerNodeRoleWrapper) roleWrapper).getVotedFor(), rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getTerm(), appendEntries(rpc), rpc.getMessageId());
            case CANDIDATE:
                //如果有两个candidate，并且另外一个candidate先成了leader
                //则当前节点退化为follower并重置选举定时器
                becomeFollower(rpc.getTerm(), null, rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getTerm(), appendEntries(rpc), rpc.getMessageId());
            case LEADER:
                log.warn("Received append entries rpc from another leader ({})", rpc.getLeaderId());
                return new AppendEntriesResult(rpc.getTerm(), false, rpc.getMessageId());
            default:
                throw new IllegalStateException("Unexpected node role [" + roleWrapper.getRole() + "]");
        }
    }

    private boolean appendEntries(AppendEntriesRpc rpc) {
        AppendEntriesState state = context.getLog().appendEntriesFromLeader(rpc.getPrevLogIndex(), rpc.getPrevLogTerm(), rpc.getEntries());
        if (state.isSuccess()) {
            if (state.hasGroup()) {
                context.getGroup().updateNodes(state.getLatestGroup());
            }
            context.getLog().advanceCommitIndex(Math.min(rpc.getLeaderCommit(), rpc.getLastEntryIndex()), rpc.getTerm());//append entries from leader
            return true;
        }
        return false;
    }

    //=============================   此节点收到追加日志结果   =============================
    @Subscribe
    public void onReceiveAppendEntriesResult(AppendEntriesResultMessage message) {
        context.getTaskExecutor().execute(() -> doProcessAppendEntriesResult(message));
    }

    private void doProcessAppendEntriesResult(AppendEntriesResultMessage resultMessage) {
        AppendEntriesResult result = resultMessage.getResult();
        //如果对方任期比己方大，则己方退化为follower
        if (result.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive append entries result, term in result > my term");
            becomeFollower(result.getTerm(), null, null);
            return;
        }
        //检查己方角色
        if (roleWrapper.getRole() != NodeRole.LEADER) {
            log.warn("Received append entries result from node {}, but this node is not a leader at present, ignore", resultMessage.getSourceNodeId());
        }
        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveAppendEntriesResult(resultMessage, context.getLog().getNextIndex())) {
            return;
        }
        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.getGroup().getMember(sourceNodeId);
        if (member == null) {
            log.info("Unexpected append entries result from node {}, node maybe removed", sourceNodeId);
            return;
        }
        AppendEntriesRpc rpc = resultMessage.getRpc();
        if (result.isSuccess()) {
            //追加日志成功并且NextIndex或MatchIndex有变更，推进本地CommitIndex
            if (member.getReplicatingState().advanceNextIndexAndMatchIndex(rpc.getLastEntryIndex())) {
                List<GroupConfigEntry> groupConfigEntries = context.getLog().
                        advanceCommitIndex(context.getGroup().getNewCommitIndex(), roleWrapper.getTerm());//leader advance commit index
                if (!groupConfigEntries.isEmpty()) {
                    //应该是只有一条集群成员变更日志被提交的，毕竟如果前一条集群成员变更日志没有被提交，那么后一条根本不会被append
                    assert groupConfigEntries.size() == 1;
                    currentGroupConfigChangeTask.onLogCommitted();
                }
            }
            for (ReadTask task : readTaskMap.values()) {
                if (task.receiveSuccessAppendEntriesResult(resultMessage)) {
                    log.debug("Remove read task {}", task);
                    readTaskMap.remove(task.getRequestId());
                }
            }
            // node caught up
            if (member.getReplicatingState().getNextIndex() >= context.getLog().getNextIndex()) {
                member.stopReplicating();
                return;
            }
        } else {
            //追加日志失败，回退NextIndex
            if (!member.getReplicatingState().backOffMatchIndex()) {
                log.warn("Can not back off next index anymore, node {}", sourceNodeId);
            }
        }
        doReplicateLog(member, context.getConfig().getMaxReplicationEntries());
    }

    //=============================   收到安装日志快照请求 source: leader   =============================
    @Subscribe
    public void onReceiveInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        context.getTaskExecutor().execute(() -> context.getConnector().reply(doProcessInstallSnapshotRpc(rpcMessage), rpcMessage));
    }

    private InstallSnapshotResult doProcessInstallSnapshotRpc(InstallSnapshotRpcMessage rpcMessage) {
        InstallSnapshotRpc rpc = rpcMessage.getRpc();
        //如果对方的term比己方小，则返回己方term
        if (rpc.getTerm() < roleWrapper.getTerm()) {
            return new InstallSnapshotResult(roleWrapper.getTerm());
        }
        //如果对方的term比己方大，则己方退化为follower
        if (rpc.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive install snapshot rpc, term in rpc > my term");
            becomeFollower(rpc.getTerm(), null, rpc.getLeaderId());
        }
        InstallSnapshotState state = context.getLog().installSnapshot(rpc);
        //安装完成后应用随日志快照一起的新集群配置
        if (state.getStateName() == InstallSnapshotState.StateName.INSTALLED) {
            context.getGroup().updateNodes(state.getLastConfig());
        }
        return new InstallSnapshotResult(rpc.getTerm());
    }

    //=============================   收到安装日志快照结果 source: follower   =============================
    @Subscribe
    public void onReceiveInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        context.getTaskExecutor().execute(() -> doProcessInstallSnapshotResult(resultMessage));
    }

    private void doProcessInstallSnapshotResult(InstallSnapshotResultMessage resultMessage) {
        InstallSnapshotResult result = resultMessage.getResult();
        //如果对方term比己方term大，则己方退化为follower
        if (result.getTerm() > roleWrapper.getTerm()) {
            log.info("Receive install snapshot result, term in result > my term");
            becomeFollower(result.getTerm(), null, null);
            return;
        }
        //leader check
        if (roleWrapper.getRole() != NodeRole.LEADER) {
            log.warn("Receive install snapshot result from node {} but current node is not leader, ignore", resultMessage.getSourceNodeId());
            return;
        }
        // dispatch to new node catch up task by node id
        if (newNodeCatchUpTaskGroup.onReceiveInstallSnapshotResult(resultMessage, context.getLog().getNextIndex())) {
            return;
        }

        NodeId sourceNodeId = resultMessage.getSourceNodeId();
        GroupMember member = context.getGroup().getMember(sourceNodeId);
        if (member == null) {
            log.info("Unexpected install snapshot result from node {}, node maybe removed", sourceNodeId);
            return;
        }
        InstallSnapshotRpc rpc = resultMessage.getRpc();
        if (rpc.isDone()) {
            // snapshot is installed, update this follower's corresponding replicating state
            // and then send append entries rpc
            member.getReplicatingState().advanceNextIndexAndMatchIndex(rpc.getLastIndex());
            doReplicateLog(member, context.getConfig().getMaxReplicationEntries());
        } else {
            // transfer data
            InstallSnapshotRpc nextRpc = context.getLog().createInstallSnapshotRpc(roleWrapper.getTerm(), context.getSelfId(),
                    rpc.getOffset() + rpc.getDataLength(), context.getConfig().getSnapshotDataLength());
            context.getConnector().send(nextRpc, member.getEndpoint());
        }
    }

    //=============================   收到快照创建完毕事件 source: statemachine   =============================
    @Subscribe
    public void onSnapshotGenerated(SnapshotGeneratedEvent event) {
        context.getTaskExecutor().execute(
                () -> context.getLog().snapshotGenerated(event.getLastIncludedIndex())
        );
    }

    //=============================   创建选举定时器   =============================
    private ElectionTimeout scheduleElectionTimeout() {
        //创建请求超时定时器
        return context.getScheduler().scheduleElectionTimeout(this::electionTimeout);
    }

    //提交给选举定时器的任务
    //default scope for test
    void electionTimeout() {
        //任务是将一个任务提交给任务处理器
        context.getTaskExecutor().execute(this::doProcessElectionTimeout);
    }

    /**
     * 此节点选举定时器到时时采取的实际操作
     */
    private void doProcessElectionTimeout() {
        log.info("Election timeout.");
        // election timeout cannot happen when this node is a leader
        if (roleWrapper.getRole() == NodeRole.LEADER) {
            // should not happen
            log.error("Node {}, current role is leader, ignore election timeout.", context.getSelfId());
            return;
        }
        if (context.getMode() == NodeMode.QUIET) {
            log.info("Starts with standby mode, skip election");
        }
        //对于Follower节点来说是发起选举
        //对于Candidate节点来说是再次发起选举
        //选举term加1
        int newTerm = roleWrapper.getTerm() + 1;
        if (context.getGroup().isStandalone()) {
            //standalone单机模式，跳过选举直接成为leader
            log.info("Become leader without election due to standalone mode, term {}", newTerm);
            resetReplicatingStates();
            changeRoleTo(new LeaderNodeRoleWrapper(newTerm, scheduleLogReplicationTask()));
            context.getLog().appendEntry(newTerm);
        } else {
            if (roleWrapper.getRole() == NodeRole.FOLLOWER) {
                preVote();
            } else {
                startElection(newTerm);
            }
        }
    }

    /**
     * 以Follower身份发起预投票，如果在预投票中该节点当选Leader则马上以Candidate身份正式发起选举
     */
    private void preVote() {
        log.info("Start pre-election, term {}", roleWrapper.getTerm());
        changeRoleTo(new FollowerNodeRoleWrapper(roleWrapper.getTerm(), null, null, 1, scheduleElectionTimeout()));
        PreVoteRpc rpc = new PreVoteRpc();
        rpc.setTerm(roleWrapper.getTerm());
        rpc.setLastLogIndex(context.getLog().getLastLogIndex());
        rpc.setLastLogTerm(context.getLog().getLastLogTerm());
        context.getConnector().send(rpc, context.getGroup().getEndpointsExceptSelf());
    }

    /**
     * 正式以Candidate身份发起选举
     *
     * @param term term
     */
    private void startElection(int term) {
        log.info("Start election, term {}", term);
        //变成Candidate
        changeRoleTo(new CandidateNodeRoleWrapper(term, scheduleElectionTimeout()));
        //请求投票
        RequestVoteRpc rpc = new RequestVoteRpc();
        rpc.setTerm(term);
        rpc.setCandidateId(context.getSelfId());
        rpc.setLastLogIndex(context.getLog().getLastLogIndex());
        rpc.setLastLogTerm(context.getLog().getLastLogTerm());
        context.getConnector().send(rpc, context.getGroup().getEndpointsExceptSelf());
    }

    //=============================   创建日志复制定时器（心跳） || 发送安装日志快照rpc   =============================
    private LogReplicationTask scheduleLogReplicationTask() {
        return context.getScheduler().scheduleLogReplicationTask(this::replicateLog);
    }

    //default scope for test
    void replicateLog() {
        context.getTaskExecutor().execute(this::doReplicateLog);
    }

    private void doReplicateLog() {
        // just advance commit index if is unique node
        if (context.getGroup().isStandalone()) {
            context.getLog().advanceCommitIndex(context.getLog().getNextIndex() - 1, roleWrapper.getTerm());//standalone
            return;
        }
        log.debug("Replicate log");
        for (GroupMember member : context.getGroup().getReplicationTargets()) {
            if (member.shouldReplicate(context.getConfig().getLogReplicationReadTimeout())) {
                doReplicateLog(member, context.getConfig().getMaxReplicationEntries());
            } else {
                log.debug("Node {} is replicating when receiving append entries result, skip replication task", member.getEndpoint().getId());
            }
        }
    }

    private void doReplicateLog(GroupMember member, int maxEntries) {
        member.replicateNow();
        try {
            AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(roleWrapper.getTerm(), context.getSelfId(), member.getReplicatingState().getNextIndex(), maxEntries);
            context.getConnector().send(rpc, member.getEndpoint());
        } catch (EntryInSnapshotException e) {
            log.debug("log entry {} in snapshot, replicate with install snapshot RPC", member.getReplicatingState().getNextIndex());
            InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(roleWrapper.getTerm(), context.getSelfId(), 0, context.getConfig().getSnapshotDataLength());
            context.getConnector().send(rpc, member.getEndpoint());
        }
    }

    //=============================   通用   =============================

    /**
     * 对{@link NodeImpl#changeRoleTo(AbstractNodeRoleWrapper)}的封装.
     * {@code preVoteCount}默认为{@code 0}。
     * {@code scheduleElectionTimeout}默认为{@code true}.
     *
     * @param term     term
     * @param votedFor 投票给谁
     * @param leaderId the id of leader
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId) {
        becomeFollower(term, votedFor, leaderId, 0, true);
    }

    /**
     * 对{@link NodeImpl#changeRoleTo(AbstractNodeRoleWrapper)}的封装。
     * {@code preVoteCount}默认为{@code 0}。
     *
     * @param term                    term
     * @param votedFor                投票给谁
     * @param leaderId                the id of leader
     * @param scheduleElectionTimeout true表示通过此方法成为的follower会选举超时
     *                                false表示通过此方法成为follower之后不会选举超时
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, boolean scheduleElectionTimeout) {
        becomeFollower(term, votedFor, leaderId, 0, scheduleElectionTimeout);
    }

    /**
     * 对{@link NodeImpl#changeRoleTo(AbstractNodeRoleWrapper)}的封装.
     * {@code scheduleElectionTimeout}默认为{@code true}.
     *
     * @param term          term
     * @param votedFor      投票给谁
     * @param leaderId      the id of leader
     * @param preVotesCount 预选已获得的票数
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, int preVotesCount) {
        becomeFollower(term, votedFor, leaderId, preVotesCount, true);
    }

    /**
     * 对{@link NodeImpl#changeRoleTo(AbstractNodeRoleWrapper)}的封装.
     *
     * @param term                    term
     * @param votedFor                投票给谁
     * @param leaderId                the id of leader
     * @param preVotesCount           预选已获得的票数
     * @param scheduleElectionTimeout true表示通过此方法成为的follower会选举超时
     *                                false表示通过此方法成为follower之后不会选举超时
     */
    private void becomeFollower(int term, NodeId votedFor, NodeId leaderId, int preVotesCount, boolean scheduleElectionTimeout) {
        roleWrapper.cancelTimeoutOrTask();
        ElectionTimeout electionTimeout = scheduleElectionTimeout ? scheduleElectionTimeout() : ElectionTimeout.TIME_OUT_NEVER;
        changeRoleTo(new FollowerNodeRoleWrapper(term, votedFor, leaderId, preVotesCount, electionTimeout));
    }

    private void changeRoleTo(AbstractNodeRoleWrapper role) {
        log.debug("Node {}, role state changed -> {}", context.getSelfId(), role);
        this.roleWrapper = role;
    }

    /**
     * 重置其他所有节点的复制进度，在此节点成为Leader后调用
     */
    private void resetReplicatingStates() {
        context.getGroup().resetReplicatingStates(context.getLog().getNextIndex());
    }

    private void ensureLeader() {
        if (roleWrapper.getRole() == NodeRole.LEADER) {
            return;
        }
        NodeId leaderId = roleWrapper.getLeaderId(context.getSelfId());
        NodeEndpoint endpoint = leaderId != null ? context.getGroup().findMember(leaderId).getEndpoint() : null;
        throw new NotLeaderException(roleWrapper.getRole(), endpoint);
    }

    @Nullable
    private GroupConfigChangeTaskResult awaitPreviousGroupConfigChangeTask() {
        try {
            currentGroupConfigChangeTaskReference.awaitDone(context.getConfig().getPreviousGroupConfigChangeTimeout());
            return null;
        } catch (InterruptedException ignored) {
            return GroupConfigChangeTaskResult.ERROR;
        } catch (TimeoutException ignored) {
            log.info("previous cannot complete within timeout");
            return GroupConfigChangeTaskResult.TIMEOUT;
        }
    }
    //=============================   getter   =============================

    //default scope for test
    AbstractNodeRoleWrapper getRoleWrapper() {
        return roleWrapper;
    }

    //default scope for test
    NodeContext getContext() {
        return context;
    }

    //=============================   inner class   =============================
    private class NewNodeCatchUpTaskContextImpl implements NewNodeCatchUpTaskContext {

        @Override
        public void replicateLog(NodeEndpoint endpoint) {
            context.getTaskExecutor().execute(
                    () -> doReplicateLog(endpoint, context.getLog().getNextIndex())
            );
        }

        @Override
        public void doReplicateLog(NodeEndpoint endpoint, int nextIndex) {
            try {
                AppendEntriesRpc rpc = context.getLog().createAppendEntriesRpc(
                        roleWrapper.getTerm(),
                        context.getSelfId(),
                        nextIndex,
                        context.getConfig().getMaxReplicationEntriesForNewNode()
                );
                context.getConnector().send(rpc, endpoint);
            } catch (EntryInSnapshotException ignored) {
                // change to install snapshot rpc if entry in snapshot
                log.debug("Log entry {} in snapshot, replicate with install snapshot RPC", nextIndex);
                InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(
                        roleWrapper.getTerm(),
                        context.getSelfId(),
                        0,
                        context.getConfig().getSnapshotDataLength()
                );
                context.getConnector().send(rpc, endpoint);
            }
        }

        @Override
        public void sendInstallSnapshot(NodeEndpoint endpoint, int offset) {
            InstallSnapshotRpc rpc = context.getLog().createInstallSnapshotRpc(
                    roleWrapper.getTerm(),
                    context.getSelfId(),
                    offset,
                    context.getConfig().getSnapshotDataLength()
            );
            context.getConnector().send(rpc, endpoint);
        }

        @Override
        public void done(NewNodeCatchUpTask task) {
            // remove task from group
            newNodeCatchUpTaskGroup.remove(task);
        }
    }

    private class GroupConfigChangeTaskContextImpl implements GroupConfigChangeTaskContext {

        @Override
        public void addNode(NodeEndpoint endpoint, int nextIndex, int matchIndex) {
            context.getTaskExecutor().execute(() -> {
                context.getLog().appendEntryForAddNode(roleWrapper.getTerm(), context.getGroup().getEndpoints(), endpoint);
                assert !context.getSelfId().equals(endpoint.getId());
                context.getGroup().addNode(endpoint, nextIndex, matchIndex);
                for (ReadTask task : readTaskMap.values()) {
                    task.nodeAdded(endpoint.getId());
                }
                doReplicateLog();
            });
        }

        @Override
        public void downgradeSelf() {
            log.info("Downgrade self.");
            becomeFollower(roleWrapper.getTerm(), null, null, false);
        }

        @Override
        public void removeNode(NodeId nodeId) {
            context.getTaskExecutor().execute(() -> {
                Set<NodeEndpoint> nodeEndpoints = context.getGroup().getEndpoints();
                context.getLog().appendEntryForRemoveNode(roleWrapper.getTerm(), nodeEndpoints, nodeId);
                context.getGroup().removeNode(nodeId);
                for (ReadTask task : readTaskMap.values()) {
                    task.nodeRemoved(nodeId);
                }
                doReplicateLog();
            });
        }

        @Override
        public NodeId getSelfId() {
            return context.getSelfId();
        }

        @Override
        public void done() {
            // clear current group config change
            synchronized (NodeImpl.this) {
                currentGroupConfigChangeTask = GroupConfigChangeTask.NO_TASK;
                currentGroupConfigChangeTaskReference = new FixedResultGroupConfigTaskReference(GroupConfigChangeTaskResult.OK);
            }
        }
    }
}