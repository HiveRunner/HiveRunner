package com.klarna.hiverunner.sql.split;

import java.util.StringTokenizer;

public interface Context {
	StringTokenizer tokenizer();
	String statement();
	void append(String chars);
	void append(Consumer consumer);
	void flush();
}
