package com.klarna.hiverunner.sql.split;

/**
 * Removes all white space up to and including the newlines closest to the a sequence of non whitespace characters. The
 * aim here is to preserve the indentation of statements within scripts.
 */
enum NewLineUtil {

  INSTANCE;

  static String removeLeadingTrailingNewLines(String in) {
    String[] split = in.split("[\n|\r]");
    if (split.length == 1) {
      return split[0];
    }
    for (int i = 0; i < split.length; i++) {
      if (!split[i].trim().isEmpty()) {
        return split[i];
      }
    }
    return "";
  }

}
