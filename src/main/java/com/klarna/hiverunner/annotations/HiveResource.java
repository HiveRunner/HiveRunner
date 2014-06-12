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
 * Marks a field to contain test data input. The field might either be of type String, File or Path.
 * The data will be copied into the specified target file by the HiveRunner engine.
 * <p/>
 * Please refer to {@link com.klarna.hiverunner.HelloHiveRunner} for further details.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveResource {

    /**
     * Specifies where the data should be made available in hdfs.
     * Please refer to {@link com.klarna.hiverunner.HelloHiveRunner} for further details.
     */
    String targetFile();
}
