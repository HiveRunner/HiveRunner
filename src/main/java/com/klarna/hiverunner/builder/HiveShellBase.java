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

package com.klarna.hiverunner.builder;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.data.InsertIntoTable;
import com.klarna.hiverunner.sql.StatementsSplitter;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * HiveShell implementation delegating to HiveServerContainer
 */
class HiveShellBase implements HiveShell {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveShellBase.class);
    private static final String DEFAULT_NULL_REPRESENTATION = "NULL";
    private static final String DEFAULT_ROW_VALUE_DELIMTER = "\t";

    protected boolean started = false;

    protected final HiveServerContainer hiveServerContainer;

    protected final Map<String, String> hiveConf;
    protected final Map<String, String> hiveVars;
    protected final List<String> setupScripts;
    protected final List<HiveResource> resources;
    protected final List<String> scriptsUnderTest;
    protected final CommandShellEmulation commandShellEmulation;


    HiveShellBase(HiveServerContainer hiveServerContainer,
                  Map<String, String> hiveConf,
                  List<String> setupScripts,
                  List<HiveResource> resources,
                  List<String> scriptsUnderTest,
                  CommandShellEmulation commandShellEmulation) {
        this.hiveServerContainer = hiveServerContainer;
        this.hiveConf = hiveConf;
        this.setupScripts = new ArrayList<>(setupScripts);
        this.resources = new ArrayList<>(resources);
        this.scriptsUnderTest = new ArrayList<>(scriptsUnderTest);
        this.hiveVars = new HashMap<>();
        this.commandShellEmulation = commandShellEmulation;

    }

    @Override
    public List<String> executeQuery(String hql) {
        return executeQuery(hql, DEFAULT_ROW_VALUE_DELIMTER, DEFAULT_NULL_REPRESENTATION);
    }

    @Override
    public List<String> executeQuery(String hql, String rowValuesDelimitedBy, String replaceNullWith) {
        assertStarted();

        List<Object[]> resultSet = executeStatement(hql);
        List<String> result = new ArrayList<>();
        for (Object[] objects : resultSet) {
            result.add(Joiner.on(rowValuesDelimitedBy).useForNull(replaceNullWith).join(objects));
        }
        return result;
    }

    @Override
    public List<Object[]> executeStatement(String hql) {
        return executeStatementWithCommandShellEmulation(hql);
    }
    
    private List<Object[]> executeStatementWithCommandShellEmulation(String hql) {
      return hiveServerContainer.executeStatement(commandShellEmulation.transformStatement(hql));
    }

    @Override
    public void execute(String hql) {
        assertStarted();
        executeScriptWithCommandShellEmulation(hql);
    }

    @Override
    public void execute(File file) {
        assertStarted();
        execute(Charset.defaultCharset(), file);
    }

    @Override
    public void execute(Path path) {
        assertStarted();
        execute(Charset.defaultCharset(), path);
    }

    @Override
    public void execute(Charset charset, File file) {
        assertStarted();
        execute(charset, Paths.get(file.toURI()));
    }

    @Override
    public void execute(Charset charset, Path path) {
        assertStarted();
        assertFileExists(path);
        try {
            execute(new String(Files.readAllBytes(path), charset));
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read setup script file '" + path + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void start() {
        assertNotStarted();
        started = true;

        hiveServerContainer.init(hiveConf, hiveVars);

        executeSetupScripts();

        prepareResources();

        executeScriptsUnderTest();
    }

    @Override
    public void addSetupScript(String script) {
        assertNotStarted();
        setupScripts.add(script);
    }

    @Override
    public void addSetupScripts(Charset charset, Path... scripts) {
        assertNotStarted();
        for (Path script : scripts) {
            assertFileExists(script);
            try {
                String join = new String(Files.readAllBytes(script), charset);
                setupScripts.add(join);
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        "Unable to read setup script file '" + script + "': " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void addSetupScripts(Charset charset, File... scripts) {
        Path[] paths = new Path[scripts.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(scripts[i].toURI());
        }
        addSetupScripts(charset, paths);
    }

    @Override
    public void addSetupScripts(File... scripts) {
        addSetupScripts(Charset.defaultCharset(), scripts);
    }

    @Override
    public void addSetupScripts(Path... scripts) {
        addSetupScripts(Charset.defaultCharset(), scripts);
    }

    @Override
    public TemporaryFolder getBaseDir() {
        return hiveServerContainer.getBaseDir();
    }

    @Override
    public String expandVariableSubstitutes(String expression) {
        assertStarted();
        HiveConf hiveConf = getHiveConf();
        Preconditions.checkNotNull(hiveConf);
        return hiveServerContainer.getVariableSubstitution().substitute(hiveConf, expression);
    }

    @Override
    public void setProperty(String key, String value) {
        setHiveConfValue(key, value);
    }

    @Override
    public void setHiveConfValue(String key, String value) {
        assertNotStarted();
        hiveConf.put(key, value);
    }

    @Override
    public HiveConf getHiveConf() {
        assertStarted();
        return hiveServerContainer.getHiveConf();
    }

    @Override
    public OutputStream getResourceOutputStream(String targetFile) {
        try {
            assertNotStarted();
            HiveResource resource = new HiveResource(targetFile);
            resources.add(resource);
            OutputStream hiveShellStateAwareOutputStream = createPreStartOutputStream(resource.getOutputStream());
            return hiveShellStateAwareOutputStream;
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void setHiveVarValue(String var, String value) {
        assertNotStarted();
        hiveVars.put(var, value);
    }

    @Override
    public void addResource(String targetFile, String data) {
        try {
            assertNotStarted();
            resources.add(new HiveResource(targetFile, data));
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void addResource(String targetFile, Path sourceFile) {
        try {
            assertNotStarted();
            assertFileExists(sourceFile);
            resources.add(new HiveResource(targetFile, sourceFile));
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void addResource(String targetFile, File sourceFile) {
        addResource(targetFile, Paths.get(sourceFile.toURI()));
    }

    @Override
    public InsertIntoTable insertInto(String databaseName, String tableName) {
        assertStarted();
        return InsertIntoTable.newInstance(databaseName, tableName, getHiveConf());
    }

    private void executeSetupScripts() {
        for (String setupScript : setupScripts) {
            LOGGER.debug("Executing script: " + setupScript);
            executeScriptWithCommandShellEmulation(setupScript);
        }
    }

    private void prepareResources() {
        for (HiveResource resource : resources) {
            String expandedPath = hiveServerContainer.expandVariableSubstitutes(resource.getTargetFile());

            assertResourcePreconditions(resource, expandedPath);

            Path targetFile = Paths.get(expandedPath);

            // Create target file in the tmp dir and write test data to it.
            try {
                Files.createDirectories(targetFile.getParent());
                OutputStream targetFileOutputStream = Files.newOutputStream(targetFile, StandardOpenOption.CREATE_NEW);
                targetFileOutputStream.write(resource.getOutputStream().toByteArray());
                resource.getOutputStream().close();
                targetFileOutputStream.close();
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to create resource target file: " + targetFile + " (" + resource.getTargetFile() + "): "
                                + e.getMessage(), e);
            }

            LOGGER.debug("Created hive resource " + targetFile);

        }
    }


    private void executeScriptsUnderTest() {
        for (String script : scriptsUnderTest) {
            try {
              executeScriptWithCommandShellEmulation(script);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to executeScript '" + script + "': " + e.getMessage(), e);
            }
        }
    }

    private void executeScriptWithCommandShellEmulation(String script) {
          hiveServerContainer.executeScript(commandShellEmulation.transformScript(script));
    }
    
    protected final void assertResourcePreconditions(HiveResource resource, String expandedPath) {
        String unexpandedPropertyPattern = ".*\\$\\{.*\\}.*";
        boolean isUnexpanded = !expandedPath.matches(unexpandedPropertyPattern);

        Preconditions.checkArgument(isUnexpanded, "File path %s contains "
                + "unresolved references. Original arg was: %s", expandedPath, resource.getTargetFile());

        boolean isTargetFileWithinTestDir = expandedPath.startsWith(
                hiveServerContainer.getBaseDir().getRoot().getAbsolutePath());

        Preconditions.checkArgument(isTargetFileWithinTestDir,
                "All resource target files should be created in a subdirectory to the test case basedir %s : %s",
                hiveServerContainer.getBaseDir().getRoot().getAbsolutePath(), resource.getTargetFile());
    }

    protected final void assertFileExists(Path file) {
        Preconditions.checkNotNull(file, "File argument is null");
        Preconditions.checkArgument(Files.exists(file), "File %s does not exist", file);
        Preconditions.checkArgument(Files.isRegularFile(file), "%s is not a file", file);
    }

    protected final void assertNotStarted() {
        Preconditions.checkState(!started, "HiveShell was already started");
    }


    protected final void assertStarted() {
        Preconditions.checkState(started, "HiveShell was not started");
    }

    private OutputStream createPreStartOutputStream(final ByteArrayOutputStream resourceOutputStream) {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                // It should not be possible to write to the stream after the shell has been started.
                assertNotStarted();
                resourceOutputStream.write(b);
            }
        };
    }

	@Override
	public List<String> executeQuery(File script) {
		return executeQuery(Charset.defaultCharset(), script);
	}

	@Override
	public List<String> executeQuery(Path script) {
		return executeQuery(Charset.defaultCharset(), script);
	}

	@Override
	public List<String> executeQuery(Charset charset, File script) {
		return executeQuery(charset, script, DEFAULT_ROW_VALUE_DELIMTER, DEFAULT_NULL_REPRESENTATION);
	}

	@Override
	public List<String> executeQuery(Charset charset, Path script) {
		return executeQuery(charset, script, DEFAULT_ROW_VALUE_DELIMTER, DEFAULT_NULL_REPRESENTATION);
	}

	@Override
	public List<String> executeQuery(File script, String rowValuesDelimitedBy, String replaceNullWith) {
		return executeQuery(Charset.defaultCharset(), script, rowValuesDelimitedBy, replaceNullWith);
	}

	@Override
	public List<String> executeQuery(Path script, String rowValuesDelimitedBy, String replaceNullWith) {
		return executeQuery(Charset.defaultCharset(), script, rowValuesDelimitedBy, replaceNullWith);
	}

	@Override
	public List<String> executeQuery(Charset charset, File script, String rowValuesDelimitedBy,
			String replaceNullWith) {
		return executeQuery(charset, Paths.get(script.toURI()), rowValuesDelimitedBy, replaceNullWith);
	}

	@Override
	public List<String> executeQuery(Charset charset, Path script, String rowValuesDelimitedBy,
			String replaceNullWith) {
		assertStarted();
		assertFileExists(script);
		try {
			String statements = new String(Files.readAllBytes(script), charset);
			List<String> splitStatements = StatementsSplitter.splitStatements(statements);
			if (splitStatements.size() != 1) {
				throw new IllegalArgumentException("Script '" + script + "' must contain a single valid statement.");
			}
			String statement = splitStatements.get(0);
			return executeQuery(statement, rowValuesDelimitedBy, replaceNullWith);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read setup script file '" + script + "': " + e.getMessage(),
					e);
		}
	}

}
