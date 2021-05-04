
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.hiverunner/hiverunner/badge.svg?subject=io.github.hiverunner:hiverunner)](https://maven-badges.herokuapp.com/maven-central/io.github.hiverunner/hiverunner) 
[![Build](https://github.com/HiveRunner/hiverunner/workflows/build/badge.svg)](https://github.com/HiveRunner/HiveRunner/actions?query=workflow:"build")
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

![ScreenShot](/images/HiveRunnerSplash.png)

# HiveRunner

Welcome to HiveRunner - Zero installation open source unit testing of Hive applications.

[Watch the HiveRunner teaser on youtube!](http://youtu.be/B7yEAHwgi2w)

Welcome to the open source project HiveRunner. HiveRunner is a unit test framework based on JUnit (4 & 5) and enables TDD development of Hive SQL without the need for any installed dependencies. All you need is to add HiveRunner to your pom.xml as any other library and you're good to go.

HiveRunner is under constant development. It is used extensively by many companies. Please feel free to suggest improvements both as pull requests and as written requests.


## Overview

HiveRunner enables you to write Hive SQL as releasable tested artifacts. It will require you to parametrize and modularize Hive SQL in order to make it testable. The bits and pieces of code should then be wired together with some orchestration/workflow/build tool of your choice, to be runnable in your environment (e.g. Oozie, Pentaho, Talend, Maven, etc…) 

So, even though your current Hive SQL probably won't run off the shelf within HiveRunner, we believe the enforced testability and enabling of a TDD workflow will do as much good to the scripting world of SQL as it has for the Java community.

# Cook Book

## 1. Include HiveRunner

HiveRunner is published to [Maven Central](https://search.maven.org/search?q=hiverunner). To start to use it, add a dependency to HiveRunner to your pom file:

    <dependency>
        <groupId>io.github.hiverunner</groupId>
        <artifactId>hiverunner</artifactId>
        <version>[HIVERUNNER VERSION]</version>
        <scope>test</scope>
    </dependency>

Alternatively, if you want to build from source, clone this repo and build with:

     mvn install

Then add the dependency as mentioned above.

Also explicitly add the surefire plugin and configure forkMode=always to avoid OutOfMemory when building big test suites.

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
            <forkMode>always</forkMode>
        </configuration>
    </plugin>

As an alternative if this does not solve the OOM issues, try increase the -Xmx and -XX:MaxPermSize settings. For example:

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
            <forkCount>1</forkCount>
            <reuseForks>false</reuseForks>
            <argLine>-Xmx2048m -XX:MaxPermSize=512m</argLine>
        </configuration>
    </plugin>

(please note that the forkMode option is deprecated and you should use forkCount and reuseForks instead)

With forkCount and reuseForks there is a possibility to reduce the test execution time drastically, depending on your hardware. A plugin configuration which are using one fork per CPU core and reuse threads would look like:

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
            <forkCount>1C</forkCount>
            <reuseForks>true</reuseForks>
            <argLine>-Xmx2048m -XX:MaxPermSize=512m</argLine>
        </configuration>
    </plugin>

By default, HiveRunner uses mapreduce (mr) as the execution engine for Hive. If you wish to run using Tez, set the 
System property `hiveconf_hive.execution.engine` to 'tez'.

(Any Hive conf property may be overridden by prefixing it with 'hiveconf_')
        
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>2.21.0</version>
            <configuration>
                <systemProperties>
                    <hiveconf_hive.execution.engine>tez</hiveconf_hive.execution.engine>
                    <hiveconf_hive.exec.counters.pull.interval>1000</hiveconf_hive.exec.counters.pull.interval>
                </systemProperties>
            </configuration>
        </plugin>

### Timeout
It's possible to configure HiveRunner to make tests time out after some time and retry those tests a couple of times, but only when using `StandaloneHiveRunner` as this is not available in the `HiveRunnerExtension` (from HiveRunner 5.x and up). This is to cover for the bug
https://issues.apache.org/jira/browse/TEZ-2475 that at times causes test cases to not terminate due to a lost DAG reference.
The timeout feature can be configured via the 'enableTimeout', 'timeoutSeconds' and 'timeoutRetries' properties.
A configuration which enables timeouts after 30 seconds and allows 2 retries would look like:

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
            <systemProperties>
                <enableTimeout>true</enableTimeout>
                <timeoutSeconds>30</timeoutSeconds>
                <timeoutRetries>2</timeoutRetries>
            </systemProperties>
        </configuration>
    </plugin>


### Logging

HiveRunner uses [SLF4J](https://www.slf4j.org/) so you should configure logging in your tests using any compatible logging framework.

## 2. Look at the examples

Look at the [com.klarna.hiverunner.examples.HelloHiveRunnerTest](/src/test/java/com/klarna/hiverunner/examples/HelloHiveRunnerTest.java) reference test case to get a feeling for how a typical test case looks like in JUnit5. To find JUnit4 versions of the examples, look at [com.klarna.hiverunner.examples.junit4.HelloHiveRunnerTest](/src/test/java/com/klarna/hiverunner/examples/junit4/HelloHiveRunnerTest.java).

If you're put off by the verbosity of the annotations, there's always the possibility to use HiveShell in a more interactive mode.  The [com.klarna.hiverunner.SerdeTest](/src/test/java/com/klarna/hiverunner/SerdeTest.java) adds a resource (test data) interactively with HiveShell instead of using annotations.

Annotations and interactive mode can be mixed and matched, however you'll always need to include the [com.klarna.hiverunner.annotations.HiveSQL](/src/main/java/com/klarna/hiverunner/annotations/HiveSQL.java) annotation e.g:

         @HiveSQL(files = {"serdeTest/create_table.sql", "serdeTest/hql_custom_serde.sql"}, autoStart = false)
         public HiveShell hiveShell;

Note that the *autostart = false* is needed for the interactive mode. It can be left out when running with only annotations.

### Sequence files
If you work with __sequence files__ (Or anything else than regular text files) make sure to take a look at [ResourceOutputStreamTest](/src/test/java/com/klarna/hiverunner/ResourceOutputStreamTest.java) 
for an example of how to use the new method [HiveShell](src/main/java/com/klarna/hiverunner/HiveShell.java)\#getResourceOutputStream to manage test input data. 

### Programatically create test input data

Test data can be programmatically inserted into any Hive table using `HiveShell.insertInto(...)`. This seamlessly handles different storage formats and partitioning types allowing you to focus on the data required by your test scenarios:

    hiveShell.execute("create database test_db");
    hiveShell.execute("create table test_db.test_table ("
        + "c1 string,"
        + "c2 string,"
        + "c3 string"
        + ")"
        + "partitioned by (p1 string)"
        + "stored as orc");

    hiveShell.insertInto("test_db", "test_table")
        .withColumns("c1", "p1").addRow("v1", "p1")       // add { "v1", null, null, "p1" }
        .withAllColumns().addRow("v1", "v2", "v3", "p1")  // add { "v1", "v2", "v3", "p1" }
        .copyRow().set("c1", "v4")                        // add { "v4", "v2", "v3", "p1" }
        .addRowsFromTsv(file)                             // parses TSV data out of a file resource
        .addRowsFrom(file, fileParser)                    // parses custom data out of a file resource
        .commit();

See [com.klarna.hiverunner.examples.InsertTestDataTest](/src/test/java/com/klarna/hiverunner/examples/InsertTestDataTest.java) for working examples.

## 3. Understand the order of execution

HiveRunner will in default mode set up and start the HiveShell before the test method is invoked. If autostart is set to false, the [HiveShell](/src/main/java/com/klarna/hiverunner/HiveShell.java) must be started manually from within the test method. Either way, HiveRunner will do the following steps when start is invoked:

1. Merge any [@HiveProperties](/src/main/java/com/klarna/hiverunner/annotations/HiveProperties.java) from the test case with the Hive conf
2. Start the HiveServer with the merged conf
3. Copy all [@HiveResource](/src/main/java/com/klarna/hiverunner/annotations/HiveResource.java) data into the temp file area for the test
4. Execute all fields annotated with [@HiveSetupScript](/src/main/java/com/klarna/hiverunner/annotations/HiveSetupScript.java)
5. Execute the script files given in the [@HiveSQL](/src/main/java/com/klarna/hiverunner/annotations/HiveSQL.java) annotation

The [HiveShell](/src/main/java/com/klarna/hiverunner/HiveShell.java) field annotated with [@HiveSQL](/src/main/java/com/klarna/hiverunner/annotations/HiveSQL.java) will always be injected before the test method is invoked.


# Hive version compatibility

- This version of HiveRunner is built for Hive 3.1.2.
- For Hive 2.x support please use HiveRunner 5.x.
- Command shell emulations are provided to closely match the behaviour of both the Hive CLI and Beeline interactive shells. The desired emulation can be specified in your `pom.xml` file like so: 

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>2.21.0</version>
            <configuration>
                <systemProperties>
                    <!-- Defaults to HIVE_CLI, other options include BEELINE and HIVE_CLI_PRE_V200 -->
                    <commandShellEmulator>BEELINE</commandShellEmulator>
                </systemProperties>
            </configuration>
        </plugin>

  Or provided on the command line using a system property:

      mvn -DcommandShellEmulator=BEELINE test

# Future work and Limitations

* HiveRunner does not allow the `add jar` statement. It is considered bad practice to keep environment specific code together with the business logic that targets HiveRunner. Keep environment specific stuff in separate files and use your build/orchestration/workflow tool to run the right files in the right order in the right environment. When running HiveRunner, all SerDes available on the classpath of the IDE/maven will be available.

* HiveRunner runs Hive and Hive runs on top of Hadoop, and Hadoop has limited support for Windows machines. Installing [Cygwin](http://www.cygwin.com/ "Cygwin") might help out.

* Currently the HiveServer spins up and tears down for every test method. As a performance option it should be possible to clean the HiveServer and metastore between each test method invocation. The choice should probably be exposed to the test writer. By switching between different strategies, side effects/leakage can be ruled out during test case debugging. See [#69](https://github.com/HiveRunner/HiveRunner/issues/69).

# Known Issues

### UnknownHostException
I've had issues with UnknownHostException on OS X after upgrading my system or running Docker. 
Usually a restart of my machine solved it, but last time I got some corporate 
stuff installed the restarts stopped working and I kept getting UnknownHostExceptions. 
Following this simple guide solved my problem:
http://crunchify.com/getting-java-net-unknownhostexception-nodename-nor-servname-provided-or-not-known-error-on-mac-os-x-update-your-privateetchosts-file/

### Tez queries do not terminate
Tez will at times forget the process id of a random DAG. This will cause the query to never terminate. To get around this there is 
a timeout and retry functionality implemented in HiveRunner:
 
         <plugin>
             <groupId>org.apache.maven.plugins</groupId>
             <artifactId>maven-surefire-plugin</artifactId>
             <version>2.21.0</version>
             <configuration>
                 <systemProperties>
                     <enableTimeout>true</enableTimeout>
                     <timeoutSeconds>30</timeoutSeconds>
                     <timeoutRetries>2</timeoutRetries>
                     </systemProperties>
             </configuration>
         </plugin>
         
Make sure to set the timeoutSeconds to that of your slowest test in the test suite and then add some padding.

# Contact

# Mailing List
If you would like to ask any questions about or discuss HiveRunner please join our mailing list at

  [https://groups.google.com/forum/#!forum/hive-runner-user](https://groups.google.com/forum/#!forum/hive-runner-user)

# History

This project was initially developed and maintained by [Klarna](https://klarna.github.io/) and then by [Expedia Group](https://expediagroup.github.io/) before moving to its own top-level organisation on GitHub.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2021 The HiveRunner Contributors
Copyright 2013-2021 Klarna AB
