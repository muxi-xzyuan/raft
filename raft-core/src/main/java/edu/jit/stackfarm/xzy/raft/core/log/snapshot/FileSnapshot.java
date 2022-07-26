package edu.jit.stackfarm.xzy.raft.core.log.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.jit.stackfarm.xzy.raft.core.Protos;
import edu.jit.stackfarm.xzy.raft.core.log.LogDir;
import edu.jit.stackfarm.xzy.raft.core.log.LogException;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import lombok.Getter;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * FileSnapshot在初始化时会读取文件头，并把元信息存放在私有变量中
 */
public class FileSnapshot implements Snapshot {

    @Getter
    private final LogDir logDir;
    private final File snapshotFile;
    private RandomAccessFile randomAccessFile;
    @Getter
    private int lastIncludedIndex;
    @Getter
    private int lastIncludedTerm;
    @Getter
    private Set<NodeEndpoint> lastConfig;
    private long dataStart;
    private long dataLength;

    public FileSnapshot(LogDir logDir) {
        this.logDir = logDir;
        snapshotFile = logDir.getSnapshotFile();
        readHeader(logDir.getSnapshotFile());
    }

//    public FileSnapshot(File file) {
//        snapshotFile = file;
//        readHeader(file);
//    }
//
//    public FileSnapshot(RandomAccessFile randomAccessFile) {
//        readHeader(randomAccessFile);
//    }

    @Override
    public long getDataSize() {
        return dataLength;
    }

    @Override
    @Nonnull
    public SnapshotChunk readData(int offset, int length) {
        //越界判断
        if (offset > dataLength) {
            throw new IllegalArgumentException("offset > data length");
        }
        try {
            randomAccessFile.seek(dataStart + offset);
            byte[] buffer = new byte[Math.min(length, (int) dataLength - offset)];
            int n = randomAccessFile.read(buffer);
            return new SnapshotChunk(buffer, offset + n >= dataLength);
        } catch (IOException e) {
            throw new LogException("Failed to seek or read snapshot content", e);
        }
    }

    @Override
    @Nonnull
    public InputStream getDataStream() {
        try {
            FileInputStream input = new FileInputStream(snapshotFile);
            if (dataStart > 0) {
                input.skip(dataStart);
            }
            return input;
        } catch (IOException e) {
            throw new LogException("Failed to get input stream of snapshot data", e);
        }
    }

    @Override
    public void close() {
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            throw new LogException("Failed to close file", e);
        }
    }

    private void readHeader(File file) {
        try {
            readHeader(new RandomAccessFile(file, "r"));
        } catch (FileNotFoundException e) {
            throw new LogException(e);
        }
    }

    private void readHeader(RandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
        try {
            int headerLength = randomAccessFile.readInt();
            byte[] headerBytes = new byte[headerLength];
            randomAccessFile.read(headerBytes);
            Protos.SnapshotHeader header = Protos.SnapshotHeader.parseFrom(headerBytes);
            lastIncludedIndex = header.getLastIndex();
            lastIncludedTerm = header.getLastTerm();
            lastConfig = header.getLastConfigList().stream()
                    .map(e -> new NodeEndpoint(e.getId(), e.getHost(), e.getPort()))
                    .collect(Collectors.toSet());
            dataStart = randomAccessFile.getFilePointer();
            dataLength = randomAccessFile.length() - dataStart;
        } catch (InvalidProtocolBufferException e) {
            throw new LogException("Failed to parse header of snapshot", e);
        } catch (IOException e) {
            throw new LogException("Failed to read snapshot", e);
        }
    }
}
