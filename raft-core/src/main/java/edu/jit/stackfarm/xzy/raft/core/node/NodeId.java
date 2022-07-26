package edu.jit.stackfarm.xzy.raft.core.node;

import lombok.Data;
import lombok.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.io.Serializable;

/**
 * 对于字符串的简单封装，
 * 之所以不直接使用字符串，
 * 是因为如果之后有设计变更，
 * 不用到处寻找含有服务器节点ID的字符串，
 * 只需要修改NodeId和相关代码
 */
@Data
@Immutable
public class NodeId implements Serializable {

    @NonNull
    private final String value;

    @Nonnull
    public static NodeId of(@NonNull String value) {
        return new NodeId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
