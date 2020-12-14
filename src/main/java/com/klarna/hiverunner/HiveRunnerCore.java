/**
 * Copyright (C) 2013-2019 Klarna AB
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

import static org.reflections.ReflectionUtils.withAnnotation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.builder.Script;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.reflection.ReflectionUtils;

public class HiveRunnerCore {

  /**
   * Traverses the test case annotations. Will inject a HiveShell in the test case that envelopes the HiveServer.
   */
  HiveShellContainer createHiveServerContainer(List<? extends Script> scripts, Object testCase,
      Path baseDir, HiveRunnerConfig config)
      throws IOException {

    HiveServerContext context = new StandaloneHiveServerContext(baseDir, config);

    return buildShell(scripts, testCase, config, context);
  }

  private HiveShellContainer buildShell(List<? extends Script> scripts, Object testCase, HiveRunnerConfig config,
      HiveServerContext context) throws IOException {
    HiveServerContainer hiveTestHarness = new HiveServerContainer(context);

    HiveShellBuilder hiveShellBuilder = new HiveShellBuilder();
    hiveShellBuilder.setCommandShellEmulation(config.getCommandShellEmulator());

    HiveShellField shellSetter = loadScriptUnderTest(testCase, hiveShellBuilder);
    if (scripts != null) {
      hiveShellBuilder.overrideScriptsUnderTest(scripts);
    }

    hiveShellBuilder.setHiveServerContainer(hiveTestHarness);

    loadAnnotatedResources(testCase, hiveShellBuilder);

    loadAnnotatedProperties(testCase, hiveShellBuilder);

    loadAnnotatedSetupScripts(testCase, hiveShellBuilder);

    // Build shell
    HiveShellContainer shell = hiveShellBuilder.buildShell();

    // Set shell
    shellSetter.setShell(shell);

    if (shellSetter.isAutoStart()) {
      shell.start();
    }
    return shell;
  }
  
  private HiveShellField loadScriptUnderTest(Object testCaseInstance, HiveShellBuilder hiveShellBuilder) {
    try {
      Set<Field> fields = ReflectionUtils.getAllFields(testCaseInstance.getClass(), withAnnotation(HiveSQL.class));
      Preconditions.checkState(fields.size() == 1, "Exact one field should to be annotated with @HiveSQL");
      Field field = fields.iterator().next();
      HiveSQL annotation = field.getAnnotation(HiveSQL.class);
      getScriptPaths(annotation, hiveShellBuilder);
      
      boolean isAutoStart = annotation.autoStart();
      
      return new HiveShellField() {
        @Override
        public void setShell(HiveShell shell) {
          ReflectionUtils.setField(testCaseInstance, field.getName(), shell);
        }
        @Override
        public boolean isAutoStart() {
          return isAutoStart;
        }
      };
    } catch (Throwable t) {
      throw new IllegalArgumentException("Failed to init field annotated with @HiveSQL: " + t.getMessage(), t);
    }
  }
  
  public List<Path> getScriptPaths(HiveSQL annotation, HiveShellBuilder hiveShellBuilder) throws URISyntaxException {
    List<Path> scriptPaths = new ArrayList<>();
    for (String scriptFilePath : annotation.files()) {
      Path file = Paths.get(Resources.getResource(scriptFilePath).toURI());
      assertFileExists(file);
      scriptPaths.add(file);
    }
    Charset charset = annotation.encoding().equals("") ? Charset.defaultCharset() : Charset.forName(annotation.encoding());
    hiveShellBuilder.setScriptsUnderTest(scriptPaths, charset);
    return scriptPaths;
  }

  private void assertFileExists(Path file) {
    Preconditions.checkState(Files.exists(file), "File " + file + " does not exist");
  }

  private void loadAnnotatedSetupScripts(Object testCase, HiveShellBuilder workFlowBuilder) {
    Set<Field> setupScriptFields = ReflectionUtils.getAllFields(testCase.getClass(),
        withAnnotation(HiveSetupScript.class));

    for (Field setupScriptField : setupScriptFields) {
      if (ReflectionUtils.isOfType(setupScriptField, String.class)) {
        String script = ReflectionUtils.getFieldValue(testCase, setupScriptField.getName(), String.class);
        workFlowBuilder.addSetupScript(script);
      } else if (ReflectionUtils.isOfType(setupScriptField, File.class) ||
          ReflectionUtils.isOfType(setupScriptField, Path.class)) {
        Path path = getMandatoryPathFromField(testCase, setupScriptField);
        workFlowBuilder.addSetupScript(readAll(path));
      } else {
        throw new IllegalArgumentException(
            "Field annotated with @HiveSetupScript currently only supports type String, File and Path");
      }
    }
  }

  private static String readAll(Path path) {
    try {
      return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read " + path + ": " + e.getMessage(), e);
    }
  }

  private void loadAnnotatedResources(Object testCase, HiveShellBuilder workFlowBuilder) throws IOException {
    Set<Field> fields = ReflectionUtils.getAllFields(testCase.getClass(), withAnnotation(HiveResource.class));

    for (Field resourceField : fields) {

      HiveResource annotation = resourceField.getAnnotation(HiveResource.class);
      String targetFile = annotation.targetFile();

      if (ReflectionUtils.isOfType(resourceField, String.class)) {
        String data = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), String.class);
        workFlowBuilder.addResource(targetFile, data);
      } else if (ReflectionUtils.isOfType(resourceField, File.class) ||
          ReflectionUtils.isOfType(resourceField, Path.class)) {
        Path dataFile = getMandatoryPathFromField(testCase, resourceField);
        workFlowBuilder.addResource(targetFile, dataFile);
      } else {
        throw new IllegalArgumentException(
            "Fields annotated with @HiveResource currently only supports field type String, File or Path");
      }
    }
  }

  private Path getMandatoryPathFromField(Object testCase, Field resourceField) {
    Path path;
    if (ReflectionUtils.isOfType(resourceField, File.class)) {
      File dataFile = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), File.class);
      path = Paths.get(dataFile.toURI());
    } else if (ReflectionUtils.isOfType(resourceField, Path.class)) {
      path = ReflectionUtils.getFieldValue(testCase, resourceField.getName(), Path.class);
    } else {
      throw new IllegalArgumentException(
          "Only Path or File type is allowed on annotated field " + resourceField);
    }

    Preconditions.checkArgument(Files.exists(path), "File %s does not exist", path);
    return path;
  }

  private void loadAnnotatedProperties(Object testCase, HiveShellBuilder workFlowBuilder) {
    for (Field hivePropertyField : ReflectionUtils.getAllFields(testCase.getClass(),
        withAnnotation(HiveProperties.class))) {
      Preconditions.checkState(ReflectionUtils.isOfType(hivePropertyField, Map.class),
          "Field annotated with @HiveProperties should be of type Map<String, String>");
      workFlowBuilder.putAllProperties(
          ReflectionUtils.getFieldValue(testCase, hivePropertyField.getName(), Map.class));
    }
  }

  /**
   * Used as a handle for the HiveShell field in the test case so that we may set it once the
   * HiveShell has been instantiated.
   */
  interface HiveShellField {

    void setShell(HiveShell shell);

    boolean isAutoStart();
  }
}
