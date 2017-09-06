package com.klarna.hiverunner.sql.split;

import java.util.List;
import java.util.StringTokenizer;

import com.klarna.hiverunner.sql.cli.CommandShellEmulator;

public class StatementSplitter {

	public static final String SQL_SPECIAL_CHARS = ";\"'-\n";

	private final List<TokenRule> rules;
	private final String specialChars;

	public StatementSplitter(CommandShellEmulator emulator) {
		this(emulator.splitterRules(), emulator.specialCharacters());
	}

	public StatementSplitter(List<TokenRule> rules, String specialChars) {
		this.rules = rules;
		this.specialChars = specialChars;
	}

	/**
	 * Splits expression on ';'. ';' within quotes (" or ') or comments ( -- )
	 * are ignored.
	 */
	public List<String> split(String expression) {
		StringTokenizer tokenizer = new StringTokenizer(expression, specialChars, true);
		BaseContext context = new BaseContext(tokenizer);
		while (tokenizer.hasMoreElements()) {
			String token = (String) tokenizer.nextElement();
			for (TokenRule rule : rules) {
				if (rule.triggers().contains(token) || rule.triggers().isEmpty()) {
					rule.handle(token, context);
					break;
				}
			}
		}

		// Only add statement that is not empty
		context.flush();
		return context.getStatements();
	}

}
