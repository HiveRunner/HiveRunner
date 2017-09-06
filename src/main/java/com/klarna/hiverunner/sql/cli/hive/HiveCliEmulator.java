package com.klarna.hiverunner.sql.cli.hive;

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

/**
 * Emulates CLI behaviours specific to the Hive CLI. This includes interpretation of {@code source} commands, and
 * the broken full line comment handling.
 */
public enum HiveCliEmulator implements CommandShellEmulator {
	INSTANCE;
	
	@Override
	public PreProcessor preProcessor() {
		return HiveCliPreProcessor.INSTANCE;
	}

	@Override
	public PostProcessor postProcessor(HiveSqlStatementFactory factory) {
		return new SourceCommandPostProcessor(factory);
	}

	@Override
	public String getName() {
		return "HIVE_CLI";
	}

	@Override
	public String specialCharacters() {
		return StatementSplitter.SQL_SPECIAL_CHARS;
	}

	@Override
	public List<TokenRule> splitterRules() {
		return Arrays.<TokenRule> asList(CloseStatementRule.INSTANCE,
				PreserveCommentsRule.INSTANCE, PreserveQuotesRule.INSTANCE, DefaultTokenRule.INSTANCE);

	}
}
