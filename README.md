HiveRunner
==========

An Open Source unit test framework for hadoop hive queries based on JUnit4

==========

Welcome to the open source project of HiveRunner. HiveRunner is a unit test framework based on JUnit4 and enables TDD development of HiveQL without the need of any installed dependencies. All you need is to add HiveRunner to your pom.xml as any other library and you're good to go.

- Source is currently subject to final internal review and will be pushed to this repo soon.
- A pom file will be attached for building the binary artifact and as soon as we get around to it we will try to get the artifact up on maven central.

HiveRunner is under constant development. We use it extensively in all our Hive projects. Please feel free to suggest improvments both as Pull requests and as written requests.


A word from the inventors
---------
HiveRunner is meant to enable you to write Hive SQL as releasable tested artifacts. It will require you to parametrize and modularize you HiveQL in order to make it testable. The bits and pieces of code should then be wired together with e.g. Oozie, pentaho, Talend, maven or any other orchestration/workflow/build tool of your choice to be runnable in your different environments. 

So, even though your current Hive SQL probably won't run off the shelf within HiveRunner, we believe that the enforced testability and enabling of a TDD workflow will do as much good to the script world of SQL as it has done to e.g. the Java community. 


Cook Book
==========

1. Include HiveRunner
----------
Add the dependency of HiveRunner to your pom file. If you prefer Ivy, then you're currently on your own, but then again; Since you're on Ivy, you probably know what to do anyway.

    <dependency>
        <groupId>com.klarna.hadoop</groupId>
        <artifactId>hadoop-hiveunit</artifactId>
        <version>0.5-SNAPSHOT</version>
        <scope>test</scope>
    </dependency>


2. Look at the examples
----------
Secondly: Look at the **HelloHiveRunner.java** reference test case to get a feeling for how a typical test case could look like. Note that if you're put of by the verbosity of the annotations, there's always the possibility to use HiveShell in a more interactive mode. The **SerdeTest.java** interacts with the HiveShell to add a resource (test data) instead of using the annotation.

Annotations and interactice mode can be mixed and matched, however you'll always need to include the @HiveSQL annotation e.g:

         @HiveSQL(files = {"serdeTest/create_table.sql", "serdeTest/hql_custom_serde.sql"}, autoStart = false)
         public HiveShell hiveShell;

Note that the *autostart = false* is needed for the interactive mode. It can be left out when running with only annotations.


3. Understand a little bit of the order of execution
----------
HiveRunner will in default mode setup and start the HiveShell before the test method is invoked. If autostart == false, the HiveShell must be started manually from within the test method. Either way, HiveRunner will do the following steps when start is invoked. 

1. Merge any @HiveProperties from the test case with the hive conf
2. Start the HiveServer
3. Copy all @HiveResource data into the temp file area for the test
4. Execute all fields annotated with @HiveSetupScript
5. Execute the script files given in the @HiveSQL annotation

The HiveShell field annotated with @HiveSQL will always be injected before the test method is invoked.


Future work and Limitations
============
* Currently HiveRunner is only released for Hive 0.12. If the need arrises we could probably create branches that supports other Hive versions and distribute them with maven classifiers.

* HiveRunner does not allow the add jar statement. It is considered bad practice to keep that
kind of environment specific code together with the business logic that we try to target
with HiveRunner. Keep environment specific stuff in separate files and use your build/orkestration/workflow tool to run the right files in the right order in the right environment.

* HiveRunner runs Hive and Hive runs on top of hadoop, and hadoop has limited support for windows machines. Installing [Cygwin](http://www.cygwin.com/ "Cygwin") might help out.

* Some of the HiveRunner annotations should probably be rebuilt to be more test method specific. E.g. Resources may be described on a test method basis instead as for a whole test case. Feedback is always welcome!

* Clean up the logs
    
* HiveRunner currently uses in-memory Derby as metastore. It seems to be a real performance bottleneck. We've been trying to replace it with hsqldb but has yet to get it to work.

* Currently the HiveServer is spun up and torn down for every test method. As a performance option it should be possible to instead clean the HiveServer and metastore between each test method invocation. The choice should probably be exposed to the test writer. By swithing between different strategies, sideffects/leakage could be ruled out when debugging a test case.
