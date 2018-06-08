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
package com.klarna.hiverunner.sql.cli.beeline;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Joiner;
import com.klarna.hiverunner.sql.split.StatementSplitter;

public class BeelineStatementSplitterTest {

	private StatementSplitter splitter = new StatementSplitter(BeelineEmulator.INSTANCE);

	@Test
	public void testSplitBasic() {
		String str = "foo;bar;baz";
		List<String> expected = asList("foo", "bar", "baz");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testRemoveTrailingSemiColon() {
		String str = ";foo;bar;baz;";
		List<String> expected = asList("foo", "bar", "baz");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testDiscardRedundantSemiColons() {
		String str = "a;b;;;c";
		List<String> expected = asList("a", "b", "c");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testDiscardTrailingSpace() {
		String str = "a;   b\t\n   ;  \n\tc   c;";
		List<String> expected = asList("a", "b", "c   c");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testDiscardEmptyStatements() {
		String str = "a;b;     \t\n   ;c;";
		List<String> expected = asList("a", "b", "c");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testCommentPreserved() {
		String str = "foo -- bar";
		List<String> expected = asList("foo -- bar");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testCommentWithSingleQuote() {
		String str = "foo -- b'ar";
		List<String> expected = asList("foo -- b'ar");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testCommentWithDoubleQuote() {
		String str = "foo -- b\"ar";
		List<String> expected = asList("foo -- b\"ar");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testCommentWithSemiColon() {
		String str = "foo -- b;ar";
		List<String> expected = asList("foo -- b;ar");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testMultilineStatementWithComment() {
		String str = "foo -- b;ar\nbaz";
		List<String> expected = asList("foo -- b;ar\nbaz");
		assertEquals(expected, splitter.split(str));
	}

	@Test
	public void testRealLifeExample() {
		String firstStatamenet = "CREATE TABLE serde_test (\n" + "  key STRING,\n" + "  value STRING\n" + ")\n"
				+ "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'\n" + "WITH SERDEPROPERTIES  (\n"
				+ "\"input.regex\" = \"(.*);\"                                       \n" + ")\n"
				+ "STORED AS TEXTFILE\n" + "LOCATION '${hiveconf:hadoop.tmp.dir}/serde'";

		String secondStatamenet = "select * from foobar";

		assertEquals(Arrays.asList(firstStatamenet, secondStatamenet),
				splitter.split(firstStatamenet + ";\n" + secondStatamenet + ";\n"));
	}

	@Test
	public void realLifeWithComments() {
		String firstStatement = "CREATE TABLE ${hiveconf:TARGET_SCHEMA_NAME}.pacc_pstatus (\n"
				+ "  cid\tstring, -- The cid of the transaction the balance change is connected to\n"
				+ "  create_date string , -- the date of the pstatus change\n"
				+ "  old_pstatus string, -- The pstatus before the change\n"
				+ "  new_pstatus string, -- The pstatus after the change\n"
				+ "  manual boolean -- true of the pstatus change is manual, currently false for all changes "
				+ "since we can't know about manual pstatus changes\n"
				+ "  -- PRIMARY KEY() -- there no natural primary key for this table, should we add one, e.g. "
				+ "rowno?\n" + "  )";

		assertEquals(Arrays.asList(firstStatement), splitter.split(firstStatement + ";\n"));
	}

	@Test
	public void testPreserveQuoted() {
		List<String> expected = asList("\"foo\"", "'bar'", "\"\''\"", "'\"\\\"'", "';'", "\";\"");
		String input = Joiner.on(";").join(expected);
		assertEquals(expected, splitter.split(input));
	}

	@Test
	public void beelineSqlLineCommandsAreSupported() {
		String statementA = "!run script.hql";
		String statementB = "select * from table where foo != bar";
		String statementC = "!run another_script.hql";

		List<String> expected = asList(statementA, statementB, statementC);
		String expression = statementA + '\n' + statementB + ";   " + statementC;

		assertEquals(expected, splitter.split(expression));
	}

	@Test
	public void testReadUntilEndOfLine() {
		assertEquals(singletonList("foo\nbar\n\n\nbaz"), splitter.split("foo\nbar\n\n\nbaz"));
	}

	@Test
	public void testReadQuoted() {
		String firstQuote = "\"foo;\\; b  a r\\\"\"";
		String secondQuote = "'foo;\\; \\'b  a r\\\"'";
		String expectedTail = "'\'\"foxlov  e \"";

		String expression = firstQuote + secondQuote + expectedTail;

		assertEquals(singletonList("\"foo;\\; b  a r\\\"\"'foo;\\; \\'b  a r\\\"'''\"foxlov  e \""),
				splitter.split(expression));

	}
}
