package com.klarna.hiverunner.sql;

public class HiveSqlStatement {

    public static HiveSqlStatement forStatementString(String statementString) {
        return new HiveSqlStatement(statementString.trim());
    }

    private final String statementString;

    private HiveSqlStatement(String statementString) {
        this.statementString = statementString;
    }

    public String getStatementString() {
        return statementString;
    }
}
