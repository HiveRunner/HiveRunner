package com.klarna.hiverunner.hql;

public class HiveQueryLanguageStatement {

    public static HiveQueryLanguageStatement forStatementString(String statementString) {
        return new HiveQueryLanguageStatement(statementString.trim());
    }

    private final String statementString;

    private HiveQueryLanguageStatement(String statementString) {
        this.statementString = statementString;
    }

    public String getStatementString() {
        return statementString;
    }
}
