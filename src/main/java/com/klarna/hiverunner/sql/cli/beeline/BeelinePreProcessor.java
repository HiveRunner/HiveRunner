package com.klarna.hiverunner.sql.cli.beeline;

import com.klarna.hiverunner.sql.cli.CommentUtil;
import com.klarna.hiverunner.sql.cli.PreProcessor;

/**
 * A {@link PreProcessor} that strips comments from statements and scripts.
 */
enum BeelinePreProcessor implements PreProcessor {
	INSTANCE;

	@Override
	public String script(String script) {
		return CommentUtil.stripFullLineComments(script);
	}

	@Override
	public String statement(String statement) {
		return CommentUtil.stripFullLineComments(statement);
	};
}
