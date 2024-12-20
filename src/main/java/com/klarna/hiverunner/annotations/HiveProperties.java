/**
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021 The HiveRunner Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * Marks a field to contain properties that will be appended to the HiveConf.
 * <p>
 * The field should be of type <pre>Map&lt;String, String&gt;</pre>.
 * </p><p>
 * Please refer to test class {@code com.klarna.hiverunner.examples.HelloHiveRunnerTest} for usage examples.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveProperties {
}
