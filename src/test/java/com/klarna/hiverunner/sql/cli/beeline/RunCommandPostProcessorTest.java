package com.klarna.hiverunner.sql.cli.beeline;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.nio.file.Paths;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.klarna.hiverunner.sql.HiveSqlStatement;
import com.klarna.hiverunner.sql.HiveSqlStatementFactory;
import com.klarna.hiverunner.sql.cli.AbstractImportPostProcessor;

@RunWith(MockitoJUnitRunner.class)
public class RunCommandPostProcessorTest {

	@Mock
	private HiveSqlStatement statement, importedStatement;
	@Mock
	private HiveSqlStatementFactory factory;

	private AbstractImportPostProcessor processor;

	@Before
	public void setup() {
		processor = new RunCommandPostProcessor(factory);
	}

	@Test
	public void isImport() {
		when(statement.getRawStatement()).thenReturn("!run x;");
		assertThat(processor.isImport(statement), is(true));
	}

	@Test
	public void isNotImport() {
		when(statement.getRawStatement()).thenReturn("SELECT * FROM x;");
		assertThat(processor.isImport(statement), is(false));
	}

	@Test
	public void importPathValid() {
		when(statement.getRawStatement()).thenReturn("!run x");
		assertThat(processor.getImportPath(statement), is("x"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void importPathInvalid() {
		when(statement.getRawStatement()).thenReturn("!run;");
		processor.getImportPath(statement);
	}

	@Test
	public void importStatement() {
		when(statement.getRawStatement()).thenReturn("!run x");
		List<HiveSqlStatement> expected = asList(importedStatement);
		when(factory.newInstanceForPath(Paths.get("x"))).thenReturn(expected);

		assertThat(processor.statement(statement), is(expected));
	}

	@Test
	public void generalStatement() {
		when(statement.getRawStatement()).thenReturn("SELECT * FROM x");
		List<HiveSqlStatement> expected = asList(statement);
		assertThat(processor.statement(statement), is(expected));
	}

}
