package com.klarna.hiverunner;

import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrowOnTimeout extends Statement {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThrowOnTimeout.class);

    private final Statement fOriginalStatement;

    private final long fTimeout;
    private Object target;

    public ThrowOnTimeout(Statement originalStatement, long timeout, Object target) {
        fOriginalStatement = originalStatement;
        fTimeout = timeout;
        this.target = target;
    }

    @Override
    public void evaluate() throws Throwable {

        LOGGER.info("Starting timeout monitoring ({}s) of test case {}.", fTimeout/1000, target);

        StatementThread thread = evaluateStatement();
        if (!thread.fFinished) {
            throwExceptionForUnfinishedThread(thread);
        }
    }

    private StatementThread evaluateStatement() throws InterruptedException {
        StatementThread thread = new StatementThread(fOriginalStatement);
        thread.start();
        thread.join(fTimeout);
        if (!thread.fFinished) {
            thread.recordStackTrace();
        }
        thread.interrupt();
        return thread;
    }

    private void throwExceptionForUnfinishedThread(StatementThread thread)
            throws Throwable {
        if (thread.fExceptionThrownByOriginalStatement != null) {
            throw thread.fExceptionThrownByOriginalStatement;
        } else {
            throwTimeoutException(thread);
        }
    }

    private void throwTimeoutException(StatementThread thread) throws Exception {
        Exception exception = new TimeoutException(String.format(
                "test timed out after %d milliseconds", fTimeout));
        exception.setStackTrace(thread.getRecordedStackTrace());
        throw exception;
    }

    private static class StatementThread extends Thread {
        private final Statement fStatement;

        private boolean fFinished = false;

        private Throwable fExceptionThrownByOriginalStatement = null;

        private StackTraceElement[] fRecordedStackTrace = null;

        public StatementThread(Statement statement) {
            fStatement = statement;
        }

        public void recordStackTrace() {
            fRecordedStackTrace = getStackTrace();
        }

        public StackTraceElement[] getRecordedStackTrace() {
            return fRecordedStackTrace;
        }

        @Override
        public void run() {
            try {
                fStatement.evaluate();
                fFinished = true;
            } catch (InterruptedException e) {
                // don't log the InterruptedException
            } catch (Throwable e) {
                fExceptionThrownByOriginalStatement = e;
            }
        }
    }
}