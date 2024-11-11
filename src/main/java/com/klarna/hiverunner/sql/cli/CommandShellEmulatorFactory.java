/*
 * Copyright (C) 2013-2021 Klarna AB
 * Copyright (C) 2021-2024 The HiveRunner Contributors
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
package com.klarna.hiverunner.sql.cli;

import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.PreV200HiveCliEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;

public class CommandShellEmulatorFactory {

    private CommandShellEmulatorFactory() {
    }

    public static CommandShellEmulator valueOf(String name) {
        if ("beeline".equalsIgnoreCase(name.trim())) {
            return BeelineEmulator.INSTANCE;
        } else if ("hive_cli".equalsIgnoreCase(name.trim())) {
            return HiveCliEmulator.INSTANCE;
        } else if ("hive_cli_pre_v200".equalsIgnoreCase(name.trim())) {
            return PreV200HiveCliEmulator.INSTANCE;
        }
        throw new IllegalArgumentException("Unsupported CLI: " + name);
    }
}
