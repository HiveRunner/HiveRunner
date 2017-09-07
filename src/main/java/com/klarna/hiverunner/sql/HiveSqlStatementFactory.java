package com.klarna.hiverunner.sql;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.klarna.hiverunner.sql.cli.CommandShellEmulator;
import com.klarna.hiverunner.sql.split.StatementSplitter;

public class HiveSqlStatementFactory {

	private final Charset charset;
	private final CommandShellEmulator commandShellEmulation;
	private final Path cwd;

	public HiveSqlStatementFactory(Path cwd, Charset charset, CommandShellEmulator commandShellEmulation) {
		this.cwd = cwd;
		this.charset = charset;
		this.commandShellEmulation = commandShellEmulation;
	}

	private List<HiveSqlStatement> internalNewInstanceForStatement(String statement) {
		String transformedHql = commandShellEmulation.preProcessor().statement(statement.trim());
		return commandShellEmulation.postProcessor(this).statement(new HiveSqlStatement(transformedHql));
	}

	public List<HiveSqlStatement> newInstanceForScript(String script) {
		List<HiveSqlStatement> hqlStatements = new ArrayList<>();
		List<String> statements = new StatementSplitter(commandShellEmulation)
				.split(commandShellEmulation.preProcessor().script(script));
		for (String statement : statements) {
			hqlStatements.addAll(internalNewInstanceForStatement(statement));
		}
		return hqlStatements;
	}

	public List<HiveSqlStatement> newInstanceForStatement(String statement) {
		return internalNewInstanceForStatement(statement);
	}

	public List<HiveSqlStatement> newInstanceForPath(Path path) {
		if (!path.isAbsolute()) {
			path = cwd.resolve(path);
		}
		try {
			String script = new String(Files.readAllBytes(path), charset);
			return newInstanceForScript(script);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read script file '" + path + "': " + e.getMessage(), e);
		}
	}

}
