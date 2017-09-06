package com.klarna.hiverunner.sql.cli;

import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

public class CommandShellEmulatorFactory {
	
	private CommandShellEmulatorFactory() {}
	
	public static CommandShellEmulator valueOf(String name) {
		if ("beeline".equalsIgnoreCase(name.trim())) {
			return BeelineEmulator.INSTANCE;
		} else if ("hive_cli".equalsIgnoreCase(name.trim())) {
			return HiveCliEmulator.INSTANCE;
		}
		throw new IllegalArgumentException("Unsupported CLI: " + name);
	}
}
