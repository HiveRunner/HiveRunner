/*
 * Copyright 2015-2018 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
