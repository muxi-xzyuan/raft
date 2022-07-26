package edu.jit.stackfarm.xzy.raft.core.log;

import lombok.Getter;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 代表可使用的日志代的目录
 */
class LogGeneration extends AbstractLogDir implements Comparable<LogGeneration> {

    private static final Pattern DIR_NAME_PATTERN = Pattern.compile("log-(\\d+)");
    @Getter
    private final int lastIncludedIndex;

    LogGeneration(File baseDir, int lastIncludedIndex) {
        super(new File(baseDir, generateDirName(lastIncludedIndex)));
        this.lastIncludedIndex = lastIncludedIndex;
    }

    LogGeneration(File dir) {
        super(dir);
        Matcher matcher = DIR_NAME_PATTERN.matcher(dir.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a directory name of log generation, [" + dir.getName() + "]");
        }
        lastIncludedIndex = Integer.parseInt(matcher.group(1));
    }

    static boolean isValidDirName(String dirName) {
        return DIR_NAME_PATTERN.matcher(dirName).matches();
    }

    private static String generateDirName(int lastIncludedIndex) {
        return "log-" + lastIncludedIndex;
    }

    @Override
    public int compareTo(@Nonnull LogGeneration o) {
        return Integer.compare(lastIncludedIndex, o.lastIncludedIndex);
    }

    @Override
    public String toString() {
        return "LogGeneration{" +
                "dir=" + dir +
                ", lastIncludedIndex=" + lastIncludedIndex +
                '}';
    }

}
