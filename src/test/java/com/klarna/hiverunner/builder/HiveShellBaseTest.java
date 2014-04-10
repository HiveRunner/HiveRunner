package com.klarna.hiverunner.builder;

import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveShellBaseTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void substitutedVariablesShouldBeExpanded() {
        HiveShell shell = createHiveShell("origin", "spanish");
        shell.start();
        Assert.assertEquals("The spanish fox", shell.expandVariableSubstitutes("The ${hiveconf:origin} fox"));
    }

    @Test
    public void multipleSubstitutesShouldBeExpanded() {
        HiveShell shell = createHiveShell(
                "origin", "spanish",
                "animal", "fox"
        );
        shell.start();
        Assert.assertEquals("The spanish fox", shell.expandVariableSubstitutes("The ${hiveconf:origin} ${hiveconf:animal}"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void unexpandableSubstitutesShouldThrowException() {
        HiveShell shell = createHiveShell(
                "origin", "spanish"
        );
        shell.start();
        Assert.assertEquals("The spanish ${hiveconf:animal}", shell.expandVariableSubstitutes("The ${hiveconf:origin} ${hiveconf:animal}"));
    }

    @Test
    public void nestedSubstitutesShouldBeExpanded() {
        HiveShell shell = createHiveShell(
                "origin", "${origin2}",
                "origin2", "spanish",
                "animal", "fox",
                "origin_animal", "${origin} ${animal}",
                "substitute", "origin_animal"


        );
        shell.start();
        Assert.assertEquals("The spanish fox", shell.expandVariableSubstitutes("The ${hiveconf:${hiveconf:substitute}}"));
    }

    @Test(expected = IllegalStateException.class)
    public void variableSubstitutionShouldBlowUpIfShellIsNotStarted() {
        HiveShell shell = createHiveShell("origin", "spanish");
        shell.expandVariableSubstitutes("The ${hiveconf:origin} fox");
    }

    @Test
    public void setupScriptMayBeAddedBeforeStart() throws IOException {
        HiveShell shell = createHiveShell();
        shell.addSetupScript("foo");
        shell.addSetupScripts(tempFolder.newFile("foo"));
    }

    @Test
    public void setupScriptsShouldBeExecutedAtStart() throws IOException {
        HiveShell shell = createHiveShell();
        shell.addSetupScript("foo");
        shell.addSetupScripts(tempFolder.newFile("foo"));
        shell.start();
    }


    @Test(expected = IllegalStateException.class)
    public void setupScriptMayNotBeAddedAfterShellIsStarted() {
        HiveShell shell = createHiveShell();
        shell.start();
        shell.addSetupScript("foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidFilePathShouldThrowException() {
        HiveShell shell = createHiveShell();
        shell.addSetupScripts(new File("foo"));
    }


    @Test(expected = IllegalStateException.class)
    public void setupScriptsMayNotBeAddedAfterShellIsStarted() throws IOException {
        HiveShell shell = createHiveShell();
        shell.start();
        shell.addSetupScripts(tempFolder.newFile("foo"));
    }

    private HiveShell createHiveShell(String... keyValues) {
        Map<String, String> hiveConf = MapUtils.putAll(new HashMap(), keyValues);

        HiveServerContainer container = Mockito.mock(HiveServerContainer.class);
        HiveServer.HiveServerHandler client = Mockito.mock(HiveServer.HiveServerHandler.class);
        HiveServerContext context = Mockito.mock(HiveServerContext.class);

        Mockito.when(container.getClient()).thenReturn(client);
        HiveConf conf = createHiveconf(hiveConf);
        Mockito.when(client.getHiveConf()).thenReturn(conf);
        Mockito.when(context.getHiveConf()).thenReturn(conf);

        List<String> setupScripts = Arrays.asList();
        List<HiveResource> hiveResources = Arrays.asList();
        List<String> scriptsUnderTest = Arrays.asList();

        return new HiveShellBase(container, hiveConf, context, setupScripts, hiveResources, scriptsUnderTest);
    }


    private HiveConf createHiveconf(Map<String, String> conf) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.clear();

        for (Map.Entry<String, String> keyValueEntry : conf.entrySet()) {
            hiveConf.set(keyValueEntry.getKey(), keyValueEntry.getValue());
        }
        return hiveConf;
    }



}
