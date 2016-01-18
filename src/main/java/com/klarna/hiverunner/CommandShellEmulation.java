/*
 * Copyright 2015 Klarna AB
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

import java.io.File;

/**
 * Attempt to accurately emulate the behaviours (good and bad) of different Hive shells. Currently the {@code hive}
 * interactive shell (which HiveRunner uses) has an annoying issue where it blows up on some full line comments
 * (HIVE-8396). Beeline does not suffer from this and instead simply removes them. Full line comments are stripped from
 * script files as is the case with both {@code hive -f} and {@code beeline -f}. The implementations provided here
 * replicate these modes of operation.
 */
public enum CommandShellEmulation {
  HIVE_CLI {

    private final String sourceCommand = "source";

    @Override
    public boolean isImportFileStatement(String statement) {
      // case-insensitive
      return statement.trim().toLowerCase().startsWith(sourceCommand);
    }

    @Override
    public File getImportFileFromStatement(String statement) {
      // everything after 'source' (trimmed) is considered the filename
      String filename = statement.trim().substring(sourceCommand.length()).trim();
      return new File(filename);
    }

    @Override
    public String transformStatement(String statement) {
      return statement;
    }

    @Override
    public String transformScript(String script) {
      return filterFullLineComments(script);
    }
  },
  BEELINE {

    private final String runCommand = "!run";

    @Override
    public boolean isImportFileStatement(String statement) {
      // case-sensitive
      return statement.trim().startsWith(runCommand);
    }

    @Override
    public File getImportFileFromStatement(String statement) {
      // filename cannot contain whitespace
      String[] tokens = statement.trim().split(" ");
      if (tokens.length == 2) {
        return new File(tokens[1]);
      }
      throw new IllegalArgumentException("Cannot get file to import from '" + statement + "'");
    }

    @Override
    public String transformStatement(String statement) {
      return filterFullLineComments(statement);
    }

    @Override
    public String transformScript(String script) {
      return filterFullLineComments(script);
    }
  };

  public abstract boolean isImportFileStatement(String statement);

  public abstract File getImportFileFromStatement(String statement);

  public abstract String transformStatement(String statement);

  public abstract String transformScript(String script);

  private static String filterFullLineComments(String statement) {
    StringBuilder newStatement = new StringBuilder(statement.length());
    for (String line : statement.split("\n")) {
      if (!line.trim().startsWith("--")) {
        newStatement.append(line);
        newStatement.append('\n');
      }
    }
    return newStatement.toString().trim();
  }
}
