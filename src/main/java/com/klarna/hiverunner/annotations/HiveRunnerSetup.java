package com.klarna.hiverunner.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotates a field that configures the hive runner runtime.
 * So far fields of type {@link com.klarna.hiverunner.config.HiveRunnerConfig} are supported.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveRunnerSetup {
}
