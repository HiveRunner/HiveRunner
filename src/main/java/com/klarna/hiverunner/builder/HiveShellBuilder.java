/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
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
import com.klarna.hiverunner.HiveShellContainer;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a HiveShell.
 */
public class HiveShellBuilder {
    private List<Script> scriptsUnderTest = new ArrayList<>();
    private final Map<String, String> props = new HashMap<>();
    private HiveServerContainer hiveServerContainer;
    private final List<HiveResource> resources = new ArrayList<>();
    private final List<String> setupScripts = new ArrayList<>();
    private CommandShellEmulator commandShellEmulator = HiveCliEmulator.INSTANCE;

    public void setHiveServerContainer(HiveServerContainer hiveServerContainer) {
        this.hiveServerContainer = hiveServerContainer;
    }

    public void putAllProperties(Map<String, String> props) {
        this.props.putAll(props);
    }

    public void addSetupScript(String script) {
        this.setupScripts.add(script);
    }

    public void addResource(String targetFile, Path dataFile) throws IOException {
        resources.add(new HiveResource(targetFile, dataFile));
    }

    public void addResource(String targetFile, String data) throws IOException {
        resources.add(new HiveResource(targetFile, data));
    }

    public void setScriptsUnderTest(List<Path> scriptPaths, Charset charset) {
        scriptsUnderTest.addAll(fromScriptPaths(scriptPaths, charset));
    }

    public List<Script> fromScriptPaths(List<Path> scriptPaths, Charset charset) {
        List<Script> scripts = new ArrayList<>();
        int index = 0;
        for (Path path : scriptPaths) {
            Preconditions.checkState(Files.exists(path), "File %s does not exist", path);
            try {
                String sqlText = Files.readString(path, charset);
                scripts.add(new HiveRunnerScript(index++, path, sqlText));
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to load script file '" + path + "'");
            }
        }
        return scripts;
    }

    public void setCommandShellEmulation(CommandShellEmulator commandShellEmulator) {
      this.commandShellEmulator = commandShellEmulator;
    }

    public HiveShellContainer buildShell() {
        return new HiveShellTearable(hiveServerContainer, props, setupScripts, resources, scriptsUnderTest, commandShellEmulator);
    }

    public void overrideScriptsUnderTest(List<? extends Script> scripts) {
      scriptsUnderTest = new ArrayList<>(scripts);
  }
}

