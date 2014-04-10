package com.klarna.hiverunner;

import com.google.common.io.Files;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

@RunWith(StandaloneHiveRunner.class)
public class InteractiveHiveShellTest {

    @HiveSQL(files = {}, autoStart = false)
    private HiveShell shell;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void setupScriptShouldBeExecuted() {
        shell.addSetupScript("create database foo;");
        shell.start();
        List<String> actual = shell.executeQuery("show databases");
        List<String> expected = Arrays.asList("default", "foo");
        Assert.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    @Test
    public void setupScriptsShouldBeExecuted() throws IOException {
        shell.addSetupScripts(
                createFileBasedScript("create database foo;"),
                createFileBasedScript("create table foo.bar(id int);"));
        shell.start();

        List<String> actual = shell.executeQuery("show databases");
        List<String> expected = Arrays.asList("default", "foo");
        Assert.assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        List<String> actualTable = shell.executeQuery("show tables in foo");
        List<String> expectedTable = Arrays.asList("bar");
        Assert.assertEquals(new HashSet<>(expectedTable), new HashSet<>(actualTable));
    }

    @Test
    public void setupScriptsShouldBeExecutedInOrder() throws IOException {
        shell.addSetupScripts(createFileBasedScript("create database foo;"));
        shell.addSetupScript("use foo;");
        shell.addSetupScripts(createFileBasedScript("create table bar(id int)"));
        shell.start();

        List<String> actualTable = shell.executeQuery("show tables in foo");
        List<String> expectedTable = Arrays.asList("bar");
        Assert.assertEquals(new HashSet<>(expectedTable), new HashSet<>(actualTable));
    }

    private File createFileBasedScript(String script) throws IOException {
        File file = temporaryFolder.newFile(UUID.randomUUID().toString() + ".sql");
        Files.write(script, file, Charset.defaultCharset());
        return file;
    }


}
