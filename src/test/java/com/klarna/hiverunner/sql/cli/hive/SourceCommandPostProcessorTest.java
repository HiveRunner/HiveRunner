package com.klarna.hiverunner.sql.cli.hive;

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
public class SourceCommandPostProcessorTest {

	@Mock
	private HiveSqlStatement statement, importedStatement;
	@Mock
	private HiveSqlStatementFactory factory;

	private AbstractImportPostProcessor processor;

	@Before
	public void setup() {
		processor = new SourceCommandPostProcessor(factory);
	}

	@Test
	public void isImport() {
		when(statement.getRawStatement()).thenReturn("source x");
		assertThat(processor.isImport(statement), is(true));
	}

	@Test
	public void isImportCaseInsensitive() {
		when(statement.getRawStatement()).thenReturn("SoUrCe x");
		assertThat(processor.isImport(statement), is(true));
	}

	@Test
	public void isNotImport() {
		when(statement.getRawStatement()).thenReturn("SELECT * FROM x;");
		assertThat(processor.isImport(statement), is(false));
	}

	@Test
	public void importPathValid() {
		when(statement.getRawStatement()).thenReturn("source x y z");
		assertThat(processor.getImportPath(statement), is("x y z"));
	}

	@Test
	public void importStatement() {
		when(statement.getRawStatement()).thenReturn("source x");
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
