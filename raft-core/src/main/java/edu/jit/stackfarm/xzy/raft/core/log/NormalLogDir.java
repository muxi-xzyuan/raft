package edu.jit.stackfarm.xzy.raft.core.log;

import java.io.File;

/**
 * 常规日志目录，例如正在创建或正在安装的日志代
 */
public class NormalLogDir extends AbstractLogDir {

    NormalLogDir(File dir) {
        super(dir);
    }

    @Override
    public String toString() {
        return "NormalLogDir{" +
                "dir=" + dir +
                '}';
    }

}
