package com.klarna.hiverunner.sql.cli.hive;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class HiveCliEmulatorTest {
	@Test
	public void testFullLineCommentAndSetStatementHiveCli() {
		String hql = "-- hello\nset x=1;";
		assertThat(HiveCliEmulator.INSTANCE.preProcessor().statement(hql), is(hql));
	}

	@Test
	public void testFullLineCommentStatementHiveCli() {
		String hql = "-- hello";
		assertThat(HiveCliEmulator.INSTANCE.preProcessor().statement(hql), is(hql));
	}

	@Test
	public void testFullLineCommentAndSetScriptHiveCli() {
		String hql = "-- hello\nset x=1;";
		assertThat(HiveCliEmulator.INSTANCE.preProcessor().script(hql), is("set x=1;"));
	}

	@Test
	public void testFullLineCommentScriptHiveCli() {
		String hql = "-- hello";
		assertThat(HiveCliEmulator.INSTANCE.preProcessor().script(hql), is(""));
	}

}
