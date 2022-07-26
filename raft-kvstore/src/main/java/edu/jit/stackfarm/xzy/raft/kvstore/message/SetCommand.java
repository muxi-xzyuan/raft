package edu.jit.stackfarm.xzy.raft.kvstore.message;

import com.google.common.util.concurrent.FutureCallback;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.jit.stackfarm.xzy.raft.kvstore.Protos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Data
@Slf4j
@AllArgsConstructor
public class SetCommand implements FutureCallback<Object> {

    private final String requestId;
    private final String key;
    private final byte[] value;

    public SetCommand(String key, byte[] value) {
        this(UUID.randomUUID().toString(), key, value);
    }

    public static SetCommand fromBytes(byte[] bytes) {
        try {
            Protos.SetCommand protoCommand = Protos.SetCommand.parseFrom(bytes);
            return new SetCommand(
                    protoCommand.getRequestId(),
                    protoCommand.getKey(),
                    protoCommand.getValue().toByteArray()
            );
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to deserialize set command", e);
        }
    }

    @Override
    public void onSuccess(Object result) {
        log.info("Set command succeeded.");
    }

    @Override
    public void onFailure(Throwable t) {
        log.info("Set command failed");
    }

    public byte[] toBytes() {
        return Protos.SetCommand.newBuilder()
                .setRequestId(this.requestId)
                .setKey(this.key)
                .setValue(ByteString.copyFrom(this.value)).build().toByteArray();
    }
}
