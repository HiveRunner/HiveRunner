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
 * Marks a field to refer to a setup script. The field should be of type String, File or Path.
 * If it's a String the value of the field should be the actual script, not a path.
 * <p/>
 * Please refer to {@link com.klarna.hiverunner.HelloHiveRunner} for further details.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HiveSetupScript {

}
