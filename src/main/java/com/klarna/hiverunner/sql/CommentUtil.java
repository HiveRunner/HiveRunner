package com.klarna.hiverunner.sql;

public final class CommentUtil {

  private CommentUtil() {
  }
  
  public static String filterComments(String statement) {
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
