package com.klarna.hiverunner;

import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrowOnTimeout extends Statement {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThrowOnTimeout.class);

    private final Statement originalStatement;

    private final long fTimeout;
    private Object target;

    private Throwable statementException;
    private boolean finished = false;

    public ThrowOnTimeout(Statement originalStatement, long timeout, Object target) {
        this.originalStatement = originalStatement;
        fTimeout = timeout;
        this.target = target;
    }

    @Override
    public void evaluate() throws Throwable {

        LOGGER.warn("Starting timeout monitoring ({}s) of test case {}.", fTimeout / 1000, target);

        Thread statementThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    originalStatement.evaluate();
                    finished = true;
                } catch (InterruptedException e) {
                    // don't log the InterruptedException
                } catch (Throwable e) {
                    synchronized (target) {
                        statementException = e;
                    }
                }
            }
        });

        statementThread.start();
        statementThread.join(fTimeout);

        synchronized (target) {
            if (statementException != null) {
                throw statementException;
            } else if (!finished) {
                statementThread.interrupt();
                throw new TimeoutException(String.format("test timed out after %d milliseconds", fTimeout));
            }
        }
    }
}