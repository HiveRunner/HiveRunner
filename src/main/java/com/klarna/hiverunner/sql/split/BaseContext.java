package com.klarna.hiverunner.sql.split;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

class BaseContext implements Context {

	private final StringTokenizer tokenizer;
	private final List<String> statements = new ArrayList<>();
	private String statement = "";

	BaseContext(StringTokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}

	@Override
	public void flush() {
		if (!statement.trim().isEmpty()) {
			statements.add(statement.trim());
		}
		statement = "";
	}

	@Override
	public String statement() {
		return statement;
	}

	@Override
	public StringTokenizer tokenizer() {
		return tokenizer;
	}

	@Override
	public void append(String chars) {
		statement += chars;
	}

	@Override
	public void append(Consumer consumer) {
		append(consumer.consume(this));
	}

	public List<String> getStatements() {
		return statements;
	}
}
