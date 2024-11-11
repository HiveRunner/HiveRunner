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
package com.klarna.hiverunner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.klarna.hiverunner.annotations.HiveSQL;

import java.nio.file.Paths;

@ExtendWith(HiveRunnerExtension.class)
public class SetTest {

    @HiveSQL(files = {}, autoStart = true)
    private HiveShell shell;

    /**
     *  This test doesn't actually fail but if it shows up as terminated in IntelliJ (which we can't assert on)
     *  then there is a problem.
     *
     *  See https://github.com/klarna/HiveRunner/issues/94 for more details.
     */
    @Test
    public void testWithSet() {
        this.shell.execute(Paths.get("src/test/resources/SetTest/test_with_set.hql"));
    }

}
