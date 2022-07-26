package edu.jit.stackfarm.xzy.raft.core.service;

public interface Channel {

    /**
     * 发送消息，获得响应
     *
     * @param payload 消息负载
     * @return 响应内容
     */
    Object send(Object payload);

}
