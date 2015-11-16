package com.klarna.hiverunner;

import com.klarna.hiverunner.sql.CommentUtil;

/**
 * Attempt to accurately emulate the behaviours (good and bad) of different hive interactive shells. Currently the
 * {@code hive} interactive shell (which HiveRunner uses) has an annoying issue where it blows up on some full line
 * comments (HIVE-8396). Beeline does not suffer from this and instead simply removes them. The modes provided here
 * replicate these modes of operation.
 */
public enum CompatibilityMode {
  HIVE_CLI {
    @Override
    public String transform(String statement) {
      return statement;
    }
  },
  BEELINE {
    @Override
    public String transform(String statement) {
      return CommentUtil.filterComments(statement);
    }
  };

  public abstract String transform(String statement);
}
