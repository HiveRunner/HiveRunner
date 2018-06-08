package com.klarna.hiverunner.sql.split;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.klarna.hiverunner.sql.split.StatementSplitter.SQL_SPECIAL_CHARS;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.klarna.hiverunner.sql.cli.CommandShellEmulator;

@RunWith(MockitoJUnitRunner.class)
public class StatementSplitterTest {

	@Mock
	private CommandShellEmulator emulator;

	private StatementSplitter splitter;

	@Before
	public void setupEmulator() {
		// Creates a simple emulator that understands ';' only
		when(emulator.specialCharacters()).thenReturn(SQL_SPECIAL_CHARS);
		when(emulator.splitterRules())
				.thenReturn(Arrays.<TokenRule> asList(CloseStatementRule.INSTANCE, DefaultTokenRule.INSTANCE));
		splitter = new StatementSplitter(emulator);
	}

	@Test
	public void defaultRule() {
		assertThat(splitter.split("foo"), is(asList("foo")));
	}

	@Test
	public void multipleRules() {
		assertThat(splitter.split("foo;bar;baz"), is(asList("foo", "bar", "baz")));
	}

}
