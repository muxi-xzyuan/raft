package edu.jit.stackfarm.xzy.raft.core.log;

import com.google.common.io.Files;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.io.IOException;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
abstract class AbstractLogDir implements LogDir {

    final File dir;

    @Override
    public void initialize() {
        if (!dir.exists() && !dir.mkdir()) {
            throw new LogException("Failed to create directory " + dir);
        }
        try {
            Files.touch(getEntriesFile());
            Files.touch(getEntryIndexFile());
        } catch (IOException e) {
            throw new LogException("Failed to create file", e);
        }
    }

    @Override
    public boolean exists() {
        return dir.exists();
    }

    @Override
    public File getSnapshotFile() {
        return new File(dir, RootDir.FILE_NAME_SNAPSHOT);
    }

    @Override
    public File getEntriesFile() {
        return new File(dir, RootDir.FILE_NAME_ENTRIES);
    }

    @Override
    public File getEntryIndexFile() {
        return new File(dir, RootDir.FILE_NAME_ENTRY_INDEX);
    }

    @Override
    public File get() {
        return dir;
    }

    @Override
    public boolean renameTo(LogDir logDir) {
        return dir.renameTo(logDir.get());
    }

}