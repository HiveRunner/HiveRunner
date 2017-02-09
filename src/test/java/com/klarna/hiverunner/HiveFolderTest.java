/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.klarna.hiverunner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class HiveFolderTest {

    private final FsPermission writablePermission = FsPermission.getDirDefault();
    private TemporaryFolder temporaryFolder;
    private HiveFolder hiveFolder;

    @Before
    public void setup() throws IOException {
        temporaryFolder = createTemporaryFolder();

        hiveFolder = new HiveFolder(temporaryFolder.getRoot());
    }

    @After
    public void tearDown() { temporaryFolder.delete(); }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorShouldThrowException() {
        new HiveFolder(null);
    }

    @Test
    public void testFolderPermissoinHasChanged () throws IOException {
        FsPermission previousPermission = getPermission(temporaryFolder);

        hiveFolder.markAsWritable();
        FsPermission actualPermission = getPermission(temporaryFolder);

        Assert.assertNotEquals(previousPermission, actualPermission);
    }

    @Test
    public void testMakeFolderWritable () throws IOException {
        hiveFolder.markAsWritable();

        FsPermission actualPermission = getPermission(temporaryFolder);

        Assert.assertEquals(writablePermission, actualPermission);
    }

    @Test
    public void testFolderHasSufficientPermissions () throws IOException {
        FsPermission writableHDFSDirPermission = new FsPermission((short)00733);

        hiveFolder.markAsWritable();
        FsPermission actualPermission = getPermission(temporaryFolder);

        Assert.assertEquals(writableHDFSDirPermission.toShort(), writableHDFSDirPermission.toShort() & actualPermission.toShort());
    }

    private static FsPermission getPermission(TemporaryFolder folder) throws IOException {
        Path path = new Path(folder.getRoot().getPath());
        FileSystem fileSystem = path.getFileSystem(new HiveConf());
        FileStatus fileStatus = fileSystem.getFileStatus(path);

        return fileStatus.getPermission();
    }

    private static TemporaryFolder createTemporaryFolder() throws IOException {
        TemporaryFolder folder = new TemporaryFolder(new File("C:\\hive-test"));
        folder.create();

        FileUtil.setPermission(folder.getRoot(), new FsPermission((short)000));

        return folder;
    }
}