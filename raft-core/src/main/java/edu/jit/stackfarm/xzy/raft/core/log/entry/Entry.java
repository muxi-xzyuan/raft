package edu.jit.stackfarm.xzy.raft.core.log.entry;

/**
 * 日志条目
 */
public interface Entry {

    int KIND_NO_OP = 0;
    int KIND_GENERAL = 1;
    int KIND_ADD_NODE = 3;
    int KIND_REMOVE_NODE = 4;

    /**
     * 得到日志条目类型
     */
    int getKind();

    //获取索引
    int getIndex();

    //获取term
    int getTerm();

    /**
     * 获取元数据（kind，term和index）
     */
    EntryMetaData getMetaData();

    /**
     * 获得日志负载
     */
    byte[] getCommandBytes();
}
