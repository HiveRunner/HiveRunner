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
package com.klarna.hiverunner.sql.cli;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static com.klarna.hiverunner.sql.cli.CommandShellEmulatorFactory.valueOf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.klarna.hiverunner.sql.cli.beeline.BeelineEmulator;
import com.klarna.hiverunner.sql.cli.hive.HiveCliEmulator;
import com.klarna.hiverunner.sql.cli.hive.PreV200HiveCliEmulator;

public class CommandShellEmulatorFactoryTest {

    @Test
    public void beeline() {
        assertThat(valueOf("beeline"), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
        assertThat(valueOf("BEELINE"), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
        assertThat(valueOf(" bEeLiNe  "), is(equalTo((CommandShellEmulator) BeelineEmulator.INSTANCE)));
    }

    @Test
    public void hiveCli() {
        assertThat(valueOf("hive_cli"), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
        assertThat(valueOf("HIVE_CLI"), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
        assertThat(valueOf(" hIvE_cLi  "), is(equalTo((CommandShellEmulator) HiveCliEmulator.INSTANCE)));
    }

    @Test
    public void hiveCliPreV130() {
        assertThat(valueOf("hive_cli_pre_v200"), is(equalTo((CommandShellEmulator) PreV200HiveCliEmulator.INSTANCE)));
        assertThat(valueOf("HIVE_CLI_PRE_V200"), is(equalTo((CommandShellEmulator) PreV200HiveCliEmulator.INSTANCE)));
        assertThat(valueOf(" hIvE_cLi_PrE_v200  "), is(equalTo((CommandShellEmulator) PreV200HiveCliEmulator.INSTANCE)));
    }

    @Test
    public void unknown() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            valueOf("unknown");
        });
    }
}
