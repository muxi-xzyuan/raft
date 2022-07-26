package edu.jit.stackfarm.xzy.raft.kvstore.message;

import com.google.common.util.concurrent.FutureCallback;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.Immutable;
import java.util.UUID;

@Data
@Slf4j
@Immutable
public class GetCommand implements FutureCallback<Object> {

    private final String requestId;
    private final String key;

    public GetCommand(String key) {
        this.requestId = UUID.randomUUID().toString();
        this.key = key;
    }

    @Override
    public void onSuccess(Object result) {
        log.info("Get command succeeded.");
    }

    @Override
    public void onFailure(Throwable t) {
        log.info("Get command failed.");
    }

}
