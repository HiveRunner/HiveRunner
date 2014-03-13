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

package com.klarna.hiverunner;

import org.junit.rules.TemporaryFolder;
import org.junit.runners.model.InitializationError;

/**
 * Runner to run hadoop bin in a parallel JVM. Used for debugging of the HiveRunner
 * 
 * This mode is highly experimental and might need a lot of tweeking of both the LocalModeContext as well as the pom.xml
 * to run in your environment. It is generally not used during development of HiveQL, but as said above, more so when 
 * exploring strange behaviours of the HiveRunner.
 * 
 * NOTE: Don't forget to set the hadoop bin path property
 */
public class LocalModeHiveRunner extends StandaloneHiveRunner {

    public LocalModeHiveRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    protected HiveServerContext getContext(TemporaryFolder basedir) {
        return new LocalModeContext(basedir);
    }
}
