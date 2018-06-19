package com.klarna.hiverunner.sql.split;

import static java.lang.Character.isWhitespace;

/**
 * Removes all white space up to and including the newlines closest to the a sequence of non whitespace characters. The
 * aim here is to preserve the indentation of statements within scripts.
 */
enum NewLineUtil {

  INSTANCE;

  static String removeLeadingTrailingNewLines(String in) {
    int lastLeadingNLCR = -1;
    for (int i = 0; i < in.length(); i++) {
      char c = in.charAt(i);
      if (!isWhitespace(c)) {
        break;
      }
      if (c == '\n' || c == '\r') {
        lastLeadingNLCR = i;
      }
    }

    int lastTrailingNLCR = -1;
    for (int i = in.length() - 1; i >= 0; i--) {
      char c = in.charAt(i);
      if (!isWhitespace(c)) {
        break;
      }
      if (c == '\n' || c == '\r') {
        lastTrailingNLCR = i;
      }
    }
    if (lastTrailingNLCR == -1) {
      lastTrailingNLCR = in.length();
    }
    lastLeadingNLCR++;
    if (lastLeadingNLCR >= lastTrailingNLCR + 1) {
      return "";
    }
    return in.substring(lastLeadingNLCR, lastTrailingNLCR);
  }

}
