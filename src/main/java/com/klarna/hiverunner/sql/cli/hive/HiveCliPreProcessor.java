package com.klarna.hiverunner.sql.cli.hive;

import com.klarna.hiverunner.sql.cli.CommentUtil;
import com.klarna.hiverunner.sql.cli.PreProcessor;

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
