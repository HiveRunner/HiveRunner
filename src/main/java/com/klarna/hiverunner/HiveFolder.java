package com.klarna.hiverunner;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;

public class HiveFolder {
    private final File folder;

    public HiveFolder(File folder){
        if(folder == null)
            throw new IllegalArgumentException("Folder must be non null");

        this.folder = folder;
    }

    public boolean markAsWritable() throws IOException {
        FileUtil.setPermission(folder, FsPermission.getDirDefault());
        return true;
    }
}
