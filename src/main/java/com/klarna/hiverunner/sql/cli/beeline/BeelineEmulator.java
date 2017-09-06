package com.klarna.hiverunner.sql.cli.beeline;

import java.util.Arrays;
import java.util.List;

import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.cli.PostProcessor;
import com.klarna.hiverunner.sql.cli.PreProcessor;
import com.klarna.hiverunner.sql.split.CloseStatementRule;
import com.klarna.hiverunner.sql.split.DefaultTokenRule;
import com.klarna.hiverunner.sql.split.PreserveCommentsRule;
import com.klarna.hiverunner.sql.split.PreserveQuotesRule;
import com.klarna.hiverunner.sql.split.StatementSplitter;
import com.klarna.hiverunner.sql.split.TokenRule;

public enum BeelineEmulator implements CommandShellEmulator {
	INSTANCE;

	public static final String BEELINE_SPECIAL_CHARS = "!";

	@Override
	public PreProcessor preProcessor() {
		return BeelinePreProcessor.INSTANCE;
	}

	@Override
	public PostProcessor postProcessor(HiveSqlStatementFactory factory) {
		return new RunCommandPostProcessor(factory);
	}

	@Override
	public String getName() {
		return "BEELINE";
	}

	@Override
	public String specialCharacters() {
		return StatementSplitter.SQL_SPECIAL_CHARS + BEELINE_SPECIAL_CHARS;
	}

	@Override
	public List<TokenRule> splitterRules() {
		return Arrays.<TokenRule> asList(CloseStatementRule.INSTANCE, PreserveCommentsRule.INSTANCE,
				PreserveQuotesRule.INSTANCE, SqlLineCommandRule.INSTANCE, DefaultTokenRule.INSTANCE);

	}

}
