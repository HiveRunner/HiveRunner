package com.klarna.hiverunner.sql.split;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.StringTokenizer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseContextTest {

	@Mock
	private Consumer consumer;
	
	private BaseContext context = new BaseContext(new StringTokenizer(""));

	@Test
	public void appendAndFlush() {
		context.append("abc");
		assertThat(context.getStatements(), is(Collections.<String> emptyList()));
		context.append("def");
		context.flush();
		assertThat(context.getStatements(), is(singletonList("abcdef")));
	}

	@Test
	public void statementAndFlush() {
		context.append("abc");
		assertThat(context.statement(), is("abc"));
		context.flush();
		assertThat(context.statement(), is(""));
	}
	
	@Test
	public void appendWith() {
		when(consumer.consume(context)).thenReturn("statement");
		context.appendWith(consumer);
		assertThat(context.statement(), is("statement"));
		context.flush();
		assertThat(context.getStatements(), is(singletonList("statement")));
	}
}
