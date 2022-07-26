package edu.jit.stackfarm.xzy.raft.core.log.entry.sequence;

import edu.jit.stackfarm.xzy.raft.core.log.entry.Entry;
import edu.jit.stackfarm.xzy.raft.core.log.entry.EntryFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class EntriesFile {

    private final RandomAccessFile randomAccessFile;

    public EntriesFile(File file) throws FileNotFoundException {
        randomAccessFile = new RandomAccessFile(file, "rw");
    }

    public EntriesFile(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    /**
     * 追加日志条目
     *
     * @param entry 日志条目
     * @return 此次追加的起始位置
     */
    public long appendEntry(Entry entry) throws IOException {
        //光标定位到末尾
        long offset = randomAccessFile.length();
        randomAccessFile.seek(offset);
        //写入kind，类型为int
        randomAccessFile.writeInt(entry.getKind());
        //写入index，类型为index
        randomAccessFile.writeInt(entry.getIndex());
        //写入term，类型为int
        randomAccessFile.writeInt(entry.getTerm());
        byte[] commandBytes = entry.getCommandBytes();
        //写入命令的长度，类型为int
        randomAccessFile.writeInt(commandBytes.length);
        //写入命令，类型为byte[]
        randomAccessFile.write(commandBytes);
        return offset;
    }

    /**
     * 加载日志条目
     *
     * @param offset  需要加载的日志条目的起始位置
     * @param factory 日志条目工厂
     * @return 加载的日志条目
     */
    public Entry loadEntry(long offset, EntryFactory factory) throws IOException {
        if (offset > randomAccessFile.length()) {
            throw new IllegalArgumentException("offset > length");
        }
        randomAccessFile.seek(offset);
        int kind = randomAccessFile.readInt();
        int index = randomAccessFile.readInt();
        int term = randomAccessFile.readInt();
        int commandLength = randomAccessFile.readInt();
        byte[] commandBytes = new byte[commandLength];
        randomAccessFile.read(commandBytes);
        return factory.create(kind, index, term, commandBytes);
    }

    public long size() throws IOException {
        return randomAccessFile.length();
    }

    public void clear() throws IOException {
        truncate(0L);
    }

    public void truncate(long offset) throws IOException {
        if (offset >= randomAccessFile.length()) {
            return;
        }
        randomAccessFile.setLength(offset);
    }

    public void close() throws IOException {
        randomAccessFile.close();
    }

}
