package com.klarna.hiverunner.builder;

import static com.google.common.base.Charsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Files;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.sql.HiveSqlStatement;
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
import org.mockito.runners.MockitoJUnitRunner;

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
    private ArgumentCaptor<HiveSqlStatement> hqlStatementCaptor;

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
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(file);

      verify(container).executeStatement(hql);
    }

    @Test
    public void executeScriptCharsetFile() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, file);

      verify(container).executeStatement(hql);
    }
    
    @Test
    public void executeScriptPath() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(Paths.get(file.toURI()));

      verify(container).executeStatement(hql);
    }

    @Test
    public void executeScriptCharsetPath() throws IOException {
      String hql = "use default";

      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(hql, file, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));

      verify(container).executeStatement(hql);
    }

    @Test(expected = IllegalArgumentException.class)
    public void executeScriptFileNotExists() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.hql");

      HiveShell shell = createHiveCliShell();
      shell.start();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }
    
    @Test(expected = IllegalStateException.class)
    public void executeScriptNotStarted() throws IOException {
      File file = new File(tempFolder.getRoot(), "script.hql");
      
      HiveShell shell = createHiveCliShell();
      shell.execute(UTF_8, Paths.get(file.toURI()));
    }
    
	@Test
	public void executeQueryFromFile() throws IOException {
		HiveShell shell = createHiveCliShell();
		shell.start();

		String statement = "select current_database(), NULL, 100";
		when(container.executeStatement(statement)).thenReturn(Arrays.<Object[]> asList( new Object[] {"default", null, 100}));
		String hql = statement + ";";

		File file = tempFolder.newFile("script.hql");
		Files.write(hql, file, UTF_8);

		List<String> results = shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
		assertThat(results.size(), is(1));
		assertThat(results.get(0), is("defaultxxxyyyxxx100"));
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void executeQueryFromFileMoreThanOneStatement() throws IOException {
		HiveShell shell = createHiveCliShell();
		shell.start();
		
		String hql = "use default;\nselect current_database(), NULL, 100;";
		
		File file = new File(tempFolder.getRoot(), "script.hql");
		Files.write(hql, file, UTF_8);
		
		shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void executeQueryFromFileZeroStatements() throws IOException {
		HiveShell shell = createHiveCliShell();
		shell.start();
		
		String hql = "";
		
		File file = new File(tempFolder.getRoot(), "script.hql");
		Files.write(hql, file, UTF_8);
		
		shell.executeQuery(UTF_8, Paths.get(file.toURI()), "xxx", "yyy");
	}

    @Test
    public void scriptFilesAreImportedInQueries() throws IOException {
      String hql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.hql");
      Files.write(hql, importedFile, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();

      String importHql = "source " + importedFile.getAbsolutePath();
      List<String> results = shell.executeQuery(importHql);

      assertThat(results.size(), is(0));
      verify(container).executeStatement(hql);
    }

    @Test
    public void scriptFilesAreImportedInOtherScriptsHiveCli() throws IOException {
      String hql = "use default";

      File importedFile = new File(tempFolder.getRoot(), "imported_script.hql");
      Files.write(hql, importedFile, UTF_8);

      HiveShell shell = createHiveCliShell();
      shell.start();

      String importHql = "source " + importedFile.getAbsolutePath();
      File file = new File(tempFolder.getRoot(), "script.hql");
      Files.write(importHql, file, UTF_8);

      shell.execute(file);

      verify(container).executeStatement(hql);
    }
    
    @Test
    public void scriptFilesAreImportedInOtherScriptsBeeline() throws IOException {
    	String hql = "use default";
    	
    	File importedFile = new File(tempFolder.getRoot(), "imported_script.hql");
    	Files.write(hql, importedFile, UTF_8);
    	
    	HiveShell shell = createBeelineShell();
    	shell.start();
    	
    	String importHql = "!run " + importedFile.getAbsolutePath();
    	File file = new File(tempFolder.getRoot(), "script.hql");
    	Files.write(importHql, file, UTF_8);
    	
    	shell.execute(file);
    	
    	verify(container).executeStatement(hql);
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
        Mockito.when(container.getHiveConf()).thenReturn(conf);
        Mockito.when(container.getClient()).thenReturn(client);

        HiveServerContext context = Mockito.mock(HiveServerContext.class);
        Mockito.when(context.getHiveConf()).thenReturn(conf);

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
