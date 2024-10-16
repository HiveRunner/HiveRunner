/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
import static org.reflections.ReflectionUtils.withType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.builder.Script;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.reflection.ReflectionUtils;

public class HiveRunnerExtension implements AfterEachCallback, TestInstancePostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HiveRunnerExtension.class);

  private final HiveRunnerCore core;
  private final HiveRunnerConfig config = new HiveRunnerConfig();
  private Path basedir;
  private HiveShellContainer container;
  protected List<Script> scriptsUnderTest = new ArrayList<Script>();

  public HiveRunnerExtension() {
    core = new HiveRunnerCore();
  }

  protected List<Path> getScriptPaths(HiveSQL annotation) throws URISyntaxException {
    return core.getScriptPaths(annotation);
  }

  @Override
  public void postProcessTestInstance(Object target, ExtensionContext extensionContext) {
    setupConfig(target);
    try {
      basedir = Files.createTempDirectory("hiverunner_test");
      container = createHiveServerContainer(scriptsUnderTest, target, basedir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    scriptsUnderTest = container.getScriptsUnderTest();
  }

    private void setupConfig(Object target) {
        final Set<Field> fields = ReflectionUtils
                .getAllFields(
                        target.getClass(),
                        withType(HiveRunnerConfig.class)
                                .and(
                                    withAnnotation(HiveRunnerSetup.class)
                                )
                );

    Preconditions.checkState(fields.size() <= 1,
        "Only one field of type HiveRunnerConfig should be annotated with @HiveRunnerSetup");

    if (!fields.isEmpty()) {
      config.override(ReflectionUtils
          .getFieldValue(target, fields.iterator().next().getName(), HiveRunnerConfig.class));
    }
  }

  private void tearDown(Object target) {
    if (container != null) {
      LOGGER.info("Tearing down {}", target.getClass());
      container.tearDown();
    }
    deleteTempFolder(basedir);
  }

  private void deleteTempFolder(Path directory) {
    try {
      FileUtils.deleteDirectory(directory.toFile());
    } catch (IOException e) {
      LOGGER.debug("Temporary folder was not deleted successfully: " + directory);
    }
  }

  private HiveShellContainer createHiveServerContainer(List<? extends Script> scripts, Object testCase, Path basedir)
      throws IOException {
    return core.createHiveServerContainer(scripts, testCase, basedir, config);
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    tearDown(extensionContext.getRequiredTestInstance());
  }
}
