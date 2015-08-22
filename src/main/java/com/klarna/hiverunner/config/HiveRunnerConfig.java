package com.klarna.hiverunner.config;


import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class HiveRunnerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveRunnerConfig.class);

    public static final String MAP_REDUCE = "mr";
    public static final String TEZ = "tez";

    public static final String HIVE_EXECUTION_ENGINE_PROPERTY_NAME = "hiveExecutionEngine";
    public static final String HIVE_EXECUTION_ENGINE_DEFAULT = MAP_REDUCE;

    public static final String TIMEOUT_SECONDS_PROPERTY_NAME = "timeoutSeconds";
    public static final int TIMEOUT_SECONDS_DEFAULT = 30;

    public static final String TIMEOUT_RETRIES_PROPERTY_NAME = "timeoutRetries";
    public static final int TIMEOUT_RETRIES_DEFAULT = 2;

    public static final String ENABLE_TIMEOUT_PROPERTY_NAME = "enableTimeout";
    public static final boolean ENABLE_TIMEOUT_DEFAULT = false;

    private boolean timeoutEnabled;
    private int timeoutRetries;
    private int timeoutSeconds;
    private String hiveExecutionEngine;

    public HiveRunnerConfig() {
        hiveExecutionEngine = get(HIVE_EXECUTION_ENGINE_PROPERTY_NAME, HIVE_EXECUTION_ENGINE_DEFAULT, Arrays.asList(MAP_REDUCE, TEZ));

        timeoutEnabled = get(ENABLE_TIMEOUT_PROPERTY_NAME, ENABLE_TIMEOUT_DEFAULT);
        timeoutRetries = get(TIMEOUT_RETRIES_PROPERTY_NAME, TIMEOUT_RETRIES_DEFAULT);
        timeoutSeconds = get(TIMEOUT_SECONDS_PROPERTY_NAME, TIMEOUT_SECONDS_DEFAULT);

        if (timeoutEnabled) {
            LOGGER.warn(String.format(
                    "Timeout enabled. Setting timeout to %ss and retries to %s. Configurable via system properties " +
                            "'%s', '%s' and '%s'",
                    timeoutSeconds, timeoutRetries, ENABLE_TIMEOUT_PROPERTY_NAME, TIMEOUT_RETRIES_PROPERTY_NAME, TIMEOUT_SECONDS_PROPERTY_NAME));
        } else {
            LOGGER.warn("Timeout disabled.");
        }
    }

    private String get(String property, String defaultValue,
                       List<String> validValues) {
        String value = get(property, defaultValue);
        Preconditions.checkArgument(validValues.contains(value),
                "Invalid value of system property '"+property+"': Only values '"+validValues+"' are allowed");
        return value;
    }

    private static boolean get(String property, boolean defaultValue) {
        String value = System.getProperty(property);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }

    private static String get(String property, String defaultValue) {
        String value = System.getProperty(property);
        return value == null ? defaultValue : value;
    }

    private static int get(String property, int defaultValue) {
        String value = System.getProperty(property);
        return value == null ? defaultValue : Integer.parseInt(value);
    }


    public String getHiveExecutionEngine() {
        return hiveExecutionEngine;
    }
}
