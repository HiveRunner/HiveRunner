package com.klarna.hiverunner.builder;

import static com.google.common.base.Charsets.UTF_8;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import com.google.common.io.Files;
import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveShellBaseTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private HiveServerContainer container;

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

    @Test
    public void executeScriptFile() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();
      shell.execute(file);

      verify(container).executeStatement(hql);
    }

    @Test
    public void executeScriptCharsetFile() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();
      shell.execute(UTF_8, file);

      verify(container).executeStatement(hql);
    }
    
    @Test
    public void executeScriptPath() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();
      shell.execute(Paths.get(file.toURI()));

      verify(container).executeStatement(hql);
    }

    @Test
    public void executeScriptCharsetPath() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));

      verify(container).executeStatement(hql);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeScriptFileNotExists() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.hql");

      HiveShell shell = createHiveShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }
    
    @Test(expected = IllegalStateException.class)
    public void executeScriptNotStarted() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.hql");
      
      HiveShell shell = createHiveShell();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }

    @Test
    public void scriptFilesAreImportedInQueries() throws IOException {
      String hql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.hql");
      Files.write(hql, importedFile, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();

      String importHql = "source " + importedFile.getAbsolutePath();
      List<String> results = shell.executeQuery(importHql);

      assertThat(results, is(empty()));
      verify(container).executeStatement(hql);
    }

    @Test
    public void scriptFilesAreImportedInOtherScripts() throws IOException {
      String hql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.hql");
      Files.write(hql, importedFile, UTF_8);

      HiveShell shell = createHiveShell();
      shell.start();

      String importHql = "source " + importedFile.getAbsolutePath();
      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(importHql, file, UTF_8);

      shell.execute(file);

      verify(container).executeStatement(hql);
    }

    private HiveShell createHiveShell(String... keyValues) {
        Map<String, String> hiveConf = MapUtils.putAll(new HashMap(), keyValues);
        HiveConf conf = createHiveconf(hiveConf);

        CLIService client = Mockito.mock(CLIService.class);

        container = Mockito.mock(HiveServerContainer.class);
        Mockito.when(container.getHiveConf()).thenReturn(conf);
        Mockito.when(container.getClient()).thenReturn(client);

        HiveServerContext context = Mockito.mock(HiveServerContext.class);
        Mockito.when(context.getHiveConf()).thenReturn(conf);

        List<String> setupScripts = Arrays.asList();
        List<HiveResource> hiveResources = Arrays.asList();
        List<String> scriptsUnderTest = Arrays.asList();

        return new HiveShellBase(container, hiveConf, setupScripts, hiveResources, scriptsUnderTest, CommandShellEmulation.HIVE_CLI);
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
