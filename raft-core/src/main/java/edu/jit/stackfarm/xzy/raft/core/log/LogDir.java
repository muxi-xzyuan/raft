package edu.jit.stackfarm.xzy.raft.core.log;

import java.io.File;

/**
 * 日志目录
 */
public interface LogDir {

    /**
     * 初始化目录
     */
    void initialize();

    /**
     * 目录是否存在
     *
     * @return true if directory exists, false if directory does not exists.
     */
    boolean exists();

    /**
     * 获取日志快照文件句柄
     *
     * @return the {@code File} object representing the snapshot file.
     */
    File getSnapshotFile();

    /**
     * 获取存放日志条目的文件的句柄
     *
     * @return the {@code File} object representing the entries file.
     */
    File getEntriesFile();

    /**
     * 获取日志条目索引文件的句柄
     *
     * @return the {@code File} object representing the entry index file.
     */
    File getEntryIndexFile();

    /**
     * 获取目录
     *
     * @return the {@code File} object representing the directory.
     */
    File get();

    /**
     * 重命名目录
     *
     * @param logDir a {@code LogDir} object that has a new directory name
     * @return true if rename the directory successfully, false if rename the directory failed
     */
    boolean renameTo(LogDir logDir);
}