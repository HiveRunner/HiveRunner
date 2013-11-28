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

/**
 * Wrapper for the HiveShell that allows the fwk to sugar the HiveShell with functionality that will not be exposed to
 * the test case creator.
 */
public interface HiveShellContainer extends HiveShell {

    /**
     * Should be called after execution of each test method and should tear down the test fixture leaving
     * no residue for coming test cases.
     */
    void tearDown();
}
