package com.klarna.hiverunner.hql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.klarna.hiverunner.CommandShellEmulation;
import com.klarna.hiverunner.sql.StatementsSplitter;

public class HiveQueryLanguageStatementFactory {

	private final Charset charset;
	private final CommandShellEmulation commandShellEmulation;

	public HiveQueryLanguageStatementFactory(Charset charset, CommandShellEmulation commandShellEmulation) {
		this.charset = charset;
		this.commandShellEmulation = commandShellEmulation;
	}

	public List<HiveQueryLanguageStatement> newInstanceForScript(String script) {
		List<HiveQueryLanguageStatement> hqlStatements = new ArrayList<>();
		List<String> statements = StatementsSplitter.splitStatements(commandShellEmulation.transformScript(script));
		for (String statement : statements) {
			hqlStatements.addAll(newInstanceForStatement(statement));
		}
		return hqlStatements;
	}

	public List<HiveQueryLanguageStatement> newInstanceForStatement(String statement) {
		List<HiveQueryLanguageStatement> hqlStatements = new ArrayList<>();
		String trimmedStatement = statement.trim();
		if (commandShellEmulation.isImportFileStatement(trimmedStatement)) {
			File importFile = commandShellEmulation.getImportFileFromStatement(trimmedStatement);
			Path path = Paths.get(importFile.toURI());
			hqlStatements.addAll(newInstanceForPath(path));
		} else {
			String transformedHql = commandShellEmulation.transformStatement(trimmedStatement);
			hqlStatements.add(new HiveQueryLanguageStatement(transformedHql));
		}
		return hqlStatements;
	}

	public List<HiveQueryLanguageStatement> newInstanceForPath(Path path) {
		try {
			String script = new String(Files.readAllBytes(path), charset);
			return newInstanceForScript(script);
		} catch (IOException e) {
			throw new IllegalArgumentException("Unable to read script file '" + path + "': " + e.getMessage(), e);
		}
	}

}
