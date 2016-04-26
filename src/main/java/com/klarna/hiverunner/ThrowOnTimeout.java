package com.klarna.hiverunner;

import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.commons.lang.time.StopWatch;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrowOnTimeout extends Statement {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThrowOnTimeout.class);

    private final Statement originalStatement;

    private final HiveRunnerConfig config;
    private final Object target;

    private Throwable statementException;
    private boolean finished = false;

    public ThrowOnTimeout(Statement originalStatement, HiveRunnerConfig config, Object target) {
        this.originalStatement = originalStatement;
        this.config = config;
        this.target = target;
    }

    @Override
    public void evaluate() throws Throwable {

        final StopWatch stopWatch = new StopWatch();

        if (config.isTimeoutEnabled()) {
            LOGGER.info("Starting timeout monitoring ({}s) of test case {}.", config.getTimeoutSeconds(), target);
        }

        Thread statementThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    stopWatch.start();
                    originalStatement.evaluate();
                    finished = true;
                } catch (InterruptedException e) {
                    // Ignore the InterruptedException
                    LOGGER.debug(e.getMessage(), e);
                } catch (Throwable e) {
                    synchronized (target) {
                        statementException = e;
                    }
                }
            }
        });

        statementThread.start();
        statementThread.join(config.getTimeoutSeconds() * 1000);

        synchronized (target) {
            if (statementException != null) {
                throw statementException;
            } else if (!finished) {
                if (config.isTimeoutEnabled()) {
                    statementThread.interrupt();
                    throw new TimeoutException(
                            String.format("test timed out after %d seconds", config.getTimeoutSeconds()));
                } else {
                    LOGGER.warn("Test ran for {} seconds. Timeout disabled. See class {} for configuration options.",
                            stopWatch.getTime() / 1000, HiveRunnerConfig.class.getName());
                }
            }
        }

        statementThread.join();

        if (statementException != null) {
            throw statementException;
        }
    }

    public static TestRule create(final HiveRunnerConfig config, final Object target) {
        return new TestRule() {
            @Override
            public Statement apply(Statement base, Description description) {
                return new ThrowOnTimeout(base, config, target);
            }
        };
    }
}