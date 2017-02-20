package com.klarna.hiverunner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.File;
import java.io.IOException;

public class HiveFolder {
    private final File folder;
    private final HiveConf conf;

    public HiveFolder(File folder, HiveConf config){
        if(folder == null)
            throw new IllegalArgumentException("Folder must be not null");

        if(config == null)
            throw new IllegalArgumentException("Hive configuration must be not null");

        this.folder = folder;
        this.conf = config;
    }

    public boolean markAsWritable() throws IOException {
        FsPermission writablePermission = FsPermission.getDirDefault();
        FileUtil.setPermission(folder, writablePermission);

        return hasPermission(writablePermission);
    }

    private boolean hasPermission(FsPermission permission) throws IOException {
        FsPermission current = getCurrentPermission();
        return current.equals(permission);
    }

    private FsPermission getCurrentPermission() throws IOException {
        Path path = new Path(folder.getPath());
        FileSystem fileSystem = path.getFileSystem(conf);
        FileStatus fileStatus = fileSystem.getFileStatus(path);

        return fileStatus.getPermission();
    }
}
