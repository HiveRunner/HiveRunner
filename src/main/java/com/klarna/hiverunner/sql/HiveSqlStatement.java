package com.klarna.hiverunner.sql;

public class HiveSqlStatement {

    private final String statementString;

    HiveSqlStatement(String statementString) {
        this.statementString = statementString.trim();
    }

    public String getRawStatement() {
        return statementString;
    }
}
