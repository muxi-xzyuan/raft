package edu.jit.stackfarm.xzy.raft.kvstore.server;

import com.google.protobuf.ByteString;
import edu.jit.stackfarm.xzy.raft.core.log.statemachine.AbstractSingleThreadStateMachine;
import edu.jit.stackfarm.xzy.raft.core.node.Node;
import edu.jit.stackfarm.xzy.raft.core.node.NodeAlreadyAddedException;
import edu.jit.stackfarm.xzy.raft.core.node.role.NodeRole;
import edu.jit.stackfarm.xzy.raft.core.node.task.GroupConfigChangeTaskReference;
import edu.jit.stackfarm.xzy.raft.core.service.AddNodeCommand;
import edu.jit.stackfarm.xzy.raft.core.service.RemoveNodeCommand;
import edu.jit.stackfarm.xzy.raft.kvstore.Protos;
import edu.jit.stackfarm.xzy.raft.kvstore.message.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

/**
 * kv服务入口
 */
@Slf4j
public class Service {

    private final Node node;
    //待回复的客户端请求，String: 请求ID
    private final ConcurrentMap<String, CommandRequest<?>> pendingCommands = new ConcurrentHashMap<>();
    //KV服务的数据
    private Map<String, byte[]> map = new HashMap<>();

    public Service(Node node) {
        this.node = node;
        node.registerStateMachine(new StateMachineImpl());
    }

    /**
     * 通过kv服务的数据生成日志快照
     *
     * @param map    kv服务的数据
     * @param output 面向日志快照的输出流
     */
    static void toSnapshot(Map<String, byte[]> map, OutputStream output) throws IOException {
        Protos.EntryList.Builder entryList = Protos.EntryList.newBuilder();
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            entryList.addEntries(
                    Protos.EntryList.Entry.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(ByteString.copyFrom(entry.getValue())).build()
            );
        }
        entryList.build().writeTo(output);
    }

    public void removeNode(CommandRequest<RemoveNodeCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            commandRequest.reply(redirect);
            return;
        }
        RemoveNodeCommand command = commandRequest.getCommand();
        GroupConfigChangeTaskReference taskReference = node.removeNode(command.getNodeId());
        awaitResult(taskReference, commandRequest);
    }

    public void addNode(CommandRequest<AddNodeCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            commandRequest.reply(redirect);
            return;
        }
        AddNodeCommand command = commandRequest.getCommand();
        GroupConfigChangeTaskReference taskReference;
        try {
            taskReference = this.node.addNode(command.toNodeEndpoint());
        } catch (NodeAlreadyAddedException e) {
            commandRequest.reply(new Failure(1, "Cannot add node that is already in cluster."));
            return;
        }
        awaitResult(taskReference, commandRequest);
    }

    public void get(CommandRequest<GetCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            log.info("Reply {}", redirect);
            commandRequest.reply(redirect);
            return;
        }
        GetCommand command = commandRequest.getCommand();
        log.debug("Get {}", command.getKey());
        pendingCommands.put(command.getRequestId(), commandRequest);
        node.enqueueReadIndex(command.getRequestId());
    }

    private Redirect checkLeadership() {
        NodeRole nodeRole = node.getNodeRole();
        if (nodeRole != NodeRole.LEADER) {
            return new Redirect(node.getLeaderId());
        }
        return null;
    }

    private <T> void awaitResult(GroupConfigChangeTaskReference taskReference, CommandRequest<T> commandRequest) {
        try {
            switch (taskReference.getResult(30000L)) {
                case OK:
                    commandRequest.reply(Success.INSTANCE);
                    break;
                case TIMEOUT:
                    commandRequest.reply(new Failure(101, "timeout"));
                    break;
                default:
                    commandRequest.reply(new Failure(100, "error"));
            }
        } catch (TimeoutException e) {
            commandRequest.reply(new Failure(101, "timeout"));
        } catch (InterruptedException ignored) {
            commandRequest.reply(new Failure(100, "error"));
        }
    }

    /**
     * 通过日志快照还原kv服务的数据
     *
     * @param input 面向日志快照的输入流
     * @return kv服务的数据
     */
    static Map<String, byte[]> fromSnapshot(InputStream input) throws IOException {
        Map<String, byte[]> map = new HashMap<>();
        Protos.EntryList entryList = Protos.EntryList.parseFrom(input);
        for (Protos.EntryList.Entry entry : entryList.getEntriesList()) {
            map.put(entry.getKey(), entry.getValue().toByteArray());
        }
        return map;
    }

    public void set(CommandRequest<SetCommand> commandRequest) {
        Redirect redirect = checkLeadership();
        if (redirect != null) {
            log.info("Reply {}", redirect);
            commandRequest.reply(redirect);
            return;
        }
        SetCommand command = commandRequest.getCommand();
        log.debug("Set {}", command.getKey());
        //记录请求ID和CommandRequest的映射
        this.pendingCommands.put(command.getRequestId(), commandRequest);
        //客户端连接关闭时从映射中移除
        commandRequest.addCloseListener(() -> pendingCommands.remove(command.getRequestId()));
        node.appendLog(command.toBytes());
    }

    private class StateMachineImpl extends AbstractSingleThreadStateMachine {

        // apply set command
        @Override
        protected void applyCommand(byte[] commandBytes) {
            SetCommand command = SetCommand.fromBytes(commandBytes);
            map.put(command.getKey(), command.getValue());
            CommandRequest<?> commandRequest = pendingCommands.remove(command.getRequestId());
            if (commandRequest != null) {
                // reply set command
                commandRequest.reply(Success.INSTANCE);
            }
        }

        // response get command
        @Override
        protected void onReadIndexReached(String requestId) {
            CommandRequest<?> commandRequest = pendingCommands.remove(requestId);
            if (commandRequest != null) {
                GetCommand command = (GetCommand) commandRequest.getCommand();
                commandRequest.reply(new GetCommandResponse(map.get(command.getKey())));
            }
        }

        @Override
        protected void doApplySnapshot(InputStream input) throws IOException {
            map = fromSnapshot(input);
        }

        @Override
        public boolean shouldGenerateSnapshot(int firstLogIndex, int lastApplied) {
            //两条之后生成日志快照
            return lastApplied - firstLogIndex > 1;
        }

        @Override
        public void generateSnapshot(OutputStream output) throws IOException {
            toSnapshot(map, output);
        }
    }
}
