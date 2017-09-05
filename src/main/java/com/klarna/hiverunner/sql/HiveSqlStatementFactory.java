package com.klarna.hiverunner.sql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.klarna.hiverunner.CommandShellEmulation;

public class HiveSqlStatementFactory {

	private final Charset charset;
	private final CommandShellEmulation commandShellEmulation;

	public HiveSqlStatementFactory(Charset charset, CommandShellEmulation commandShellEmulation) {
		this.charset = charset;
		this.commandShellEmulation = commandShellEmulation;
	}

	public List<HiveSqlStatement> newInstanceForScript(String script) {
		List<HiveSqlStatement> hqlStatements = new ArrayList<>();
		List<String> statements = StatementsSplitter.splitStatements(commandShellEmulation.transformScript(script));
		for (String statement : statements) {
			hqlStatements.addAll(newInstanceForStatement(statement));
		}
		return hqlStatements;
	}

	public List<HiveSqlStatement> newInstanceForStatement(String statement) {
		List<HiveSqlStatement> hqlStatements = new ArrayList<>();
		String trimmedStatement = statement.trim();
		if (commandShellEmulation.isImportFileStatement(trimmedStatement)) {
			File importFile = commandShellEmulation.getImportFileFromStatement(trimmedStatement);
			Path path = Paths.get(importFile.toURI());
			hqlStatements.addAll(newInstanceForPath(path));
		} else {
			String transformedHql = commandShellEmulation.transformStatement(trimmedStatement);
			hqlStatements.add(HiveSqlStatement.forStatementString(transformedHql));
		}
		return hqlStatements;
	}

	public List<HiveSqlStatement> newInstanceForPath(Path path) {
		try {
			String script = new String(Files.readAllBytes(path), charset);
			return newInstanceForScript(script);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read script file '" + path + "': " + e.getMessage(), e);
		}
	}

}
