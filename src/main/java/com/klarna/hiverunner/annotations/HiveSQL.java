/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.hiverunner.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Marks a field (of type HiveShell) in a unit test. This field with its annotation is mandatory.
 * The HiveRunner will set the HiveShell instance before each test method is called.
 * <p/>
 * Please refer to {@link com.klarna.hiverunner.HelloHiveRunner} for further details.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveSQL {

    /**
     * The hive sql files subject to test. Files will be executed in order
     */
    String[] files();

    /**
     * If the shell should be started automatically before the JUnit test method is called.
     * <p/>
     * If set to false this leaves the tester to do additional setup in @Before or within actual test method. However,
     * HiveShell.start() has to be called explicit when setup is done.
     */
    boolean autoStart() default true;

    /**
     * The encoding of the given files. Will default to java.nio.charset.Charset#defaultCharset
     */
    String encoding() default "";
}
