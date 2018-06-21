/**
 * Copyright (C) 2013-2018 Klarna AB
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
package com.klarna.hiverunner.builder;

import static com.google.common.base.Charsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Files;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.CLIService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class HiveShellBaseTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private HiveServerContainer container;
    @Captor
    private ArgumentCaptor<String> hiveSqlStatementCaptor;

    @Test(expected = IllegalStateException.class)
    public void variableSubstitutionShouldBlowUpIfShellIsNotStarted() {
        HiveShell shell = createHiveCliShell("origin", "spanish");
        shell.expandVariableSubstitutes("The ${hiveconf:origin} fox");
    }

    @Test
    public void setupScriptMayBeAddedBeforeStart() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.addSetupScript("foo");
        shell.addSetupScripts(tempFolder.newFile("foo"));
    }

    @Test
    public void setupScriptsShouldBeExecutedAtStart() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.addSetupScript("foo");
        shell.addSetupScripts(tempFolder.newFile("foo"));
        shell.start();
    }


    @Test(expected = IllegalStateException.class)
    public void setupScriptMayNotBeAddedAfterShellIsStarted() {
        HiveShell shell = createHiveCliShell();
        shell.start();
        shell.addSetupScript("foo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidFilePathShouldThrowException() {
        HiveShell shell = createHiveCliShell();
        shell.addSetupScripts(new File("foo"));
    }


    @Test(expected = IllegalStateException.class)
    public void setupScriptsMayNotBeAddedAfterShellIsStarted() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.start();
        shell.addSetupScripts(tempFolder.newFile("foo"));
    }

    @Test
    public void executeScriptFile() throws IOException {
      String hiveSql = "use default";

      File file = new File(tempFolder.getRoot(), "script.sql");
      Files.write(hiveSql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(file);

      verify(container).executeStatement(hiveSql);
    }

    @Test
    public void executeScriptCharsetFile() throws IOException {
      String hiveSql = "use default";

      File file = new File(tempFolder.getRoot(), "script.sql");
      Files.write(hiveSql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, file);

      verify(container).executeStatement(hiveSql);
    }
    
    @Test
    public void executeScriptPath() throws IOException {
      String hiveSql = "use default";

      File file = new File(tempFolder.getRoot(), "script.sql");
      Files.write(hiveSql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(Paths.get(file.toURI()));

      verify(container).executeStatement(hiveSql);
    }

    @Test
    public void executeScriptCharsetPath() throws IOException {
      String hiveSql = "use default";

      File file = new File(tempFolder.getRoot(), "script.sql");
      Files.write(hiveSql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));

      verify(container).executeStatement(hiveSql);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeScriptFileNotExists() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.sql");

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }
    
    @Test(expected = IllegalStateException.class)
    public void executeScriptNotStarted() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.sql");
      
      HiveShell shell = createHiveCliShell();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }
    
    @Test
    public void executeQueryFromFile() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.start();

        String statement = "select current_database(), NULL, 100";
        when(container.executeStatement(statement)).thenReturn(Arrays.<Object[]> asList( new Object[] {"default", null, 100}));
        String hiveSql = statement + ";";

        File file = tempFolder.newFile("script.sql");
        Files.write(hiveSql, file, UTF_8);

        List<String> results = shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
        assertThat(results.size(), is(1));
        assertThat(results.get(0), is("defaultxxxyyyxxx100"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void executeQueryFromFileMoreThanOneStatement() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.start();
        
        String hiveSql = "use default;\nselect current_database(), NULL, 100;";
        
        File file = new File(tempFolder.getRoot(), "script.sql");
        Files.write(hiveSql, file, UTF_8);
        
        shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void executeQueryFromFileZeroStatements() throws IOException {
        HiveShell shell = createHiveCliShell();
        shell.start();
        
        String hiveSql = "";
        
        File file = new File(tempFolder.getRoot(), "script.sql");
        Files.write(hiveSql, file, UTF_8);
        
        shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
    }

    @Test
    public void scriptFilesAreImportedInQueries() throws IOException {
      String hiveSql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.sql");
      Files.write(hiveSql, importedFile, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();

      String importhiveSql = "source " + importedFile.getAbsolutePath();
      List<String> results = shell.executeQuery(importhiveSql);

      assertThat(results.size(), is(0));
      verify(container).executeStatement(hiveSql);
    }

    @Test
    public void scriptFilesAreImportedInOtherScriptsHiveCli() throws IOException {
      String hiveSql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.sql");
      Files.write(hiveSql, importedFile, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();

      String importhiveSql = "source " + importedFile.getAbsolutePath();
      File file = new File(tempFolder.getRoot(), "script.sql");
      Files.write(importhiveSql, file, UTF_8);

      shell.execute(file);

      verify(container).executeStatement(hiveSql);
    }
    
    @Test
    public void scriptFilesAreImportedInOtherScriptsBeeline() throws IOException {
        String hiveSql = "use default";
        
        File importedFile = new File(tempFolder.getRoot(), "imported_script.sql");
        Files.write(hiveSql, importedFile, UTF_8);
        
        HiveShell shell = createBeelineShell();
        shell.start();
        
        String importhiveSql = "!run " + importedFile.getAbsolutePath();
        File file = new File(tempFolder.getRoot(), "script.sql");
        Files.write(importhiveSql, file, UTF_8);
        
        shell.execute(file);
        
        verify(container).executeStatement(hiveSql);
    }

    private HiveShell createHiveCliShell(String... keyValues) {
        return createHiveShell(HiveCliEmulator.INSTANCE, keyValues);
    }
    private HiveShell createBeelineShell(String... keyValues) {
        return createHiveShell(BeelineEmulator.INSTANCE, keyValues);
    }
    
    private HiveShell createHiveShell(CommandShellEmulator emulation, String... keyValues) {
        Map<String, String> hiveConf = MapUtils.putAll(new HashMap(), keyValues);
        HiveConf conf = createHiveconf(hiveConf);

        CLIService client = Mockito.mock(CLIService.class);

        container = Mockito.mock(HiveServerContainer.class);

        List<String> setupScripts = Arrays.asList();
        List<HiveResource> hiveResources = Arrays.asList();
        List<String> scriptsUnderTest = Arrays.asList();

        return new HiveShellBase(container, hiveConf, setupScripts, hiveResources, scriptsUnderTest, emulation);
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
