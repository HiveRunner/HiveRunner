package com.klarna.hiverunner.sql.split;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.StringTokenizer;

import org.junit.Test;

public class PreserveQuotesRuleTest {

	private static TokenRule rule = PreserveQuotesRule.INSTANCE;

	@Test
	public void singleQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("'b c' d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "'"
		assertThat(context.statement(), is("'b c'"));
	}

	@Test
	public void singleQuotesCrossLine() {
		StringTokenizer tokenizer = new StringTokenizer("'b \n c' d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "'"
		assertThat(context.statement(), is("'b \n c'"));
	}

	@Test
	public void singleEscapedQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("'b \\' c' d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "'"
		assertThat(context.statement(), is("'b \\' c'"));
	}

	@Test
	public void doubleQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("\"b c\" d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context);
		assertThat(context.statement(), is("\"b c\""));
	}

	@Test
	public void doubleQuotesCrossLine() {
		StringTokenizer tokenizer = new StringTokenizer("\"b \n c\" d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context);
		assertThat(context.statement(), is("\"b \n c\""));
	}

	@Test
	public void doubleEscapedQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("\"b \\\" c\" d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context);
		assertThat(context.statement(), is("\"b \\\" c\""));
	}

	@Test
	public void doubleSingleQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("\"b ' c\" d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context);
		assertThat(context.statement(), is("\"b ' c\""));
	}

	@Test
	public void singleDoubleQuotes() {
		StringTokenizer tokenizer = new StringTokenizer("'b \" c' d\n", " '\"", true);
		Context context = new BaseContext(tokenizer);
		rule.handle(tokenizer.nextToken(), context); // "'"
		assertThat(context.statement(), is("'b \" c'"));
	}

}
