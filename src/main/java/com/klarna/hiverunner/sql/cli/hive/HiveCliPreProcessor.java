package com.klarna.hiverunner.sql.cli.hive;

import com.klarna.hiverunner.sql.cli.CommentUtil;
import com.klarna.hiverunner.sql.cli.PreProcessor;

/**
 * A {@link PreProcessor} that strips comments from scripts only, replicating
 * Hive CLI's broken functionality as described in
 * <a href="https://issues.apache.org/jira/browse/HIVE-8396">HIVE-8396</a>. Note
 * that this was fixed in Hive 1.3.0 so we should revisit this.
 * <p>
 * Full line comments are stripped from script files as is the case with both
 * {@code hive -f} and {@code beeline -f}. The implementations provided here
 * replicate these modes of operation.
 * </p>
 */
enum HiveCliPreProcessor implements PreProcessor {
	INSTANCE;

	@Override
	public String script(String script) {
		return CommentUtil.stripFullLineComments(script);
	}

	@Override
	public String statement(String statement) {
		return statement;
	};
}
