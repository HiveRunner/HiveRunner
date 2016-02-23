package com.klarna.hiverunner.hql;

public class HiveQueryLanguageStatement {

    private final String statementString;

    HiveQueryLanguageStatement(String statementString) {
        this.statementString = statementString;
    }

    public String getStatementString() {
        return statementString;
    }
}
