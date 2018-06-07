package com.klarna.hiverunner.sql.cli;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.klarna.hiverunner.sql.cli.CommandShellEmulatorFactory.valueOf;

import org.junit.Test;

import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

public class CommandShellEmulatorFactoryTest {

	@Test
	public void beeline() {
		assertThat(valueOf("beeline"), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
		assertThat(valueOf("BEELINE"), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
		assertThat(valueOf(" bEeLiNe  "), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
	}

	@Test
	public void hiveCli() {
		assertThat(valueOf("hive_cli"), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
		assertThat(valueOf("HIVE_CLI"), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
		assertThat(valueOf(" hIvE_cLi  "), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
	}

	@Test(expected = IllegalArgumentException.class)
	public void unknown() {
		valueOf("unknown");
	}
}
