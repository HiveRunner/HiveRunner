package com.klarna.hiverunner.sql.split;

import java.util.StringTokenizer;

/**
 * Provides a means to modify and inspect the state of the parsing and splitting
 * of a script.
 */
public interface Context {
	StringTokenizer tokenizer();
	String statement();
	void append(String chars);
	void append(Consumer consumer);
	void flush();
}
