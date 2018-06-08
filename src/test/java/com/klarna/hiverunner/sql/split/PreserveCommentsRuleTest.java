package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.StringTokenizer;

import org.junit.Test;

public class PreserveCommentsRuleTest {

	private static TokenRule rule = PreserveCommentsRule.INSTANCE;

	@Test
	public void withInlineComment() {
		StringTokenizer tokenizer = new StringTokenizer("x -- a b\n", " ", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "x"
		rule.handle(tokenizer.nextToken(), context); // " "
		rule.handle(tokenizer.nextToken(), context); // "--"
		// Should find comment and read until EOL
		assertThat(context.statement(), is("x -- a b\n"));
	}
	
	@Test
	public void noComment() {
		StringTokenizer tokenizer = new StringTokenizer("x a b\n", " ", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "x"
		rule.handle(tokenizer.nextToken(), context); // " "
		rule.handle(tokenizer.nextToken(), context); // "a"
		rule.handle(tokenizer.nextToken(), context); // " "
		rule.handle(tokenizer.nextToken(), context); // "b\n"
		assertThat(context.statement(), is("x a b\n"));
	}
	
}
