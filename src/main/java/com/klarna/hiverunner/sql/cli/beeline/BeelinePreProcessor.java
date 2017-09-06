package com.klarna.hiverunner.sql.cli.beeline;

import com.klarna.hiverunner.sql.cli.CommentUtil;
import com.klarna.hiverunner.sql.cli.PreProcessor;

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
