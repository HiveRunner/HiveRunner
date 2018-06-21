/**
 * Copyright (C) 2013-2018 Klarna AB
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
package com.klarna.hiverunner.sql.split;

import java.util.Collections;
import java.util.Set;

/** A {@link TokenRule} for handling general characters. */
public enum DefaultTokenRule implements TokenRule {
    INSTANCE;
    
    @Override
    public Set<String> triggers() {
        return Collections.emptySet();
    }

    @Override
    public void handle(String token, Context context) {
        context.append(token);
    }

}
