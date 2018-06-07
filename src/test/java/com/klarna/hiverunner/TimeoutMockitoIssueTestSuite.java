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
package com.klarna.hiverunner;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.klarna.hiverunner.data.InsertIntoTableTest;

//TODO: remove this class, it is only being used to reproduce a weird issue 
// related to the interaction betweeen the below tests and a Mockito error

@RunWith(Suite.class)
// @Suite.SuiteClasses({ TimeoutAndRetryTest.class, TableDataBuilderTest.class })
@Suite.SuiteClasses({ TimeoutAndRetryTest.class, InsertIntoTableTest.class })
// @Suite.SuiteClasses({ TestMethodIntegrityTest.class, InsertIntoTableTest.class })
public class TimeoutMockitoIssueTestSuite {

}
