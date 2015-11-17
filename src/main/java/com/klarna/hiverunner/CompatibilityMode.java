package com.klarna.hiverunner;

/**
 * Attempt to accurately emulate the behaviours (good and bad) of different Hive shells. Currently the {@code hive}
 * interactive shell (which HiveRunner uses) has an annoying issue where it blows up on some full line comments
 * (HIVE-8396). Beeline does not suffer from this and instead simply removes them. Full line comments are stripped from
 * script files as is the case with both {@code hive -f} and {@code beeline -f}. The implementations provided here
 * replicate these modes of operation.
 */
public enum CompatibilityMode {
  HIVE_CLI {
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
    @Override
    public String transformStatement(String statement) {
      return filterFullLineComments(statement);
    }

    @Override
    public String transformScript(String script) {
      return filterFullLineComments(script);
    }
  };

  public abstract String transformStatement(String statement);

  public abstract String transformScript(String script);

  // Visible for testing only
  static String filterFullLineComments(String statement) {
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
