package com.klarna.hiverunner.sql.cli;

/** Utility methods for handling SQL comments. */
public final class CommentUtil {

	private CommentUtil() {
	}

	public static String stripFullLineComments(String statement) {
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
