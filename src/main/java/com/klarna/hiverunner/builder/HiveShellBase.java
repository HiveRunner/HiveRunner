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

import com.google.common.base.Preconditions;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.service.HiveServer;
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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * HiveShell implementation delegating to HiveServerContainer
 */
class HiveShellBase implements HiveShell {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected boolean started = false;

    protected final HiveServerContainer hiveServerContainer;

    protected final Map<String, String> props;
    protected final HiveServerContext context;
    protected final List<String> setupScripts;
    protected final List<HiveResource> resources;
    protected final List<String> scriptsUnderTest;


    HiveShellBase(HiveServerContainer hiveServerContainer, Map<String, String> props,
                  HiveServerContext context, List<String> setupScripts,
                  List<HiveResource> resources,
                  List<String> scriptsUnderTest) {
        this.hiveServerContainer = hiveServerContainer;
        this.props = props;
        this.context = context;
        this.setupScripts = new ArrayList<>(setupScripts);
        this.resources = new ArrayList<>(resources);
        this.scriptsUnderTest = new ArrayList<>(scriptsUnderTest);
    }

    @Override
    public List<String> executeQuery(String s) {
        assertStarted();
        return hiveServerContainer.executeQuery(s);
    }

    @Override
    public void execute(String s) {
        assertStarted();
        hiveServerContainer.executeScript(s);
    }

    @Override
    public HiveServer.HiveServerHandler getClient() {
        assertStarted();
        return hiveServerContainer.getClient();
    }

    @Override
    public void start() {
        assertNotStarted();
        started = true;

        hiveServerContainer.init(props, context);

        executeSetupScripts();

        prepareResources();

        executeScriptsUnderTest();
    }


    @Override
    public void addResource(String targetFile, String data) {
        assertNotStarted();
        resources.add(new HiveResource(targetFile, data));
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
        try {
            return new VariableSubstitution().substitute(hiveConf, expression);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Unable to expand '" + expression + "': " + e.getMessage(), e);
        }
    }


    @Override
    public void setProperty(String key, String value) {
        assertNotStarted();
        props.put(key, value);
    }

    @Override
    public HiveConf getHiveConf() {
        assertStarted();
        return hiveServerContainer.getClient().getHiveConf();
    }

    @Override
    public OutputStream getResourceOutputStream(String targetFile) {
        assertNotStarted();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStream os = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                assertNotStarted();
                byteArrayOutputStream.write(b);
            }
        };
        resources.add(new HiveResource(targetFile, byteArrayOutputStream));
        return os;
    }

    @Override
    public void addResource(String targetFile, File sourceFile) {
        addResource(targetFile, Paths.get(sourceFile.toURI()));
    }

    @Override
    public void addResource(String targetFile, Path sourceFile) {
        assertNotStarted();
        assertFileExists(sourceFile);
        resources.add(new HiveResource(targetFile, sourceFile));
    }

    private void executeSetupScripts() {
        for (String setupScript : setupScripts) {
            logger.info("Executing script: " + setupScript);
            hiveServerContainer.executeScript(setupScript);
        }
    }

    private void prepareResources() {
        for (HiveResource resource : resources) {
            String expandedPath = hiveServerContainer.expandVariableSubstitutes(resource.getTargetFile());

            assertResourcePreconditions(resource, expandedPath);

            // Create file in the tmp dir
            Path targetFile = Paths.get(expandedPath);
            try {
                if (!Files.exists(targetFile)) {
                    Files.createDirectories(targetFile.getParent());
                    Files.createFile(targetFile);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Unable to create resource file " + targetFile + ": " + e.getMessage(),
                        e);
            }

            try {
                if (resource.isStringResource()) {
                    Files.write(targetFile, resource.getStringSource().getBytes());
                } else if (resource.isFileResource()) {
                    Files.copy(resource.getFileSource(), targetFile,
                            StandardCopyOption.REPLACE_EXISTING);
                } else if (resource.isOutputStreamResource()) {
                    OutputStream targetFileOutputStream = Files.newOutputStream(targetFile);
                    targetFileOutputStream.write(resource.getOutputStream().toByteArray());
                    resource.getOutputStream().close();
                    targetFileOutputStream.close();
                }
            } catch (IOException e) {
                logger.error("Failed to create resource target file: " + targetFile + " (" + resource.getTargetFile() +
                        "): " + e.getMessage(), e);
            }

            logger.info("Created hive resource " + targetFile);

        }
    }


    private void executeScriptsUnderTest() {
        for (String script : scriptsUnderTest) {
            try {
                hiveServerContainer.executeScript(script);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to executeScript '" + script + "': " + e.getMessage(), e);
            }
        }
    }

    protected final void assertResourcePreconditions(HiveResource resource, String expandedPath) {
        String unexpandedPropertyPattern = ".*\\$\\{.*\\}.*";
        boolean isUnexpanded = !expandedPath.matches(unexpandedPropertyPattern);

        Preconditions.checkArgument(isUnexpanded, "File path %s contains "
                + "unresolved references. Original arg was: %s", expandedPath, resource.getTargetFile());

        boolean isTargetFileWithinTestDir = expandedPath.startsWith(
                hiveServerContainer.getBaseDir().getRoot().getAbsolutePath());

        Preconditions.checkArgument(isTargetFileWithinTestDir,
                "All resource target files should be created in a subdirectory to the test case basedir : %s",
                resource);
    }

    protected final void assertFileExists(Path file) {
        Preconditions.checkArgument(Files.exists(file), "File %s does not exist", file);
        Preconditions.checkArgument(Files.isRegularFile(file), "%s is not a file", file);
    }

    protected final void assertNotStarted() {
        Preconditions.checkState(!started, "HiveShell was already started");
    }

    protected final void assertStarted() {
        Preconditions.checkState(started, "HiveShell was not started");
    }

}
