[![Build Status](https://travis-ci.org/klarna/HiveRunner.svg?branch=release_to_maven_central)](https://travis-ci.org/klarna/HiveRunner)

![ScreenShot](/images/HiveRunnerSplash.png)


HiveRunner
==========

Welcome to HiveRunner - Zero installation open source unit testing of Hive applications

[Watch the HiveRunner teaser on youtube!](http://youtu.be/B7yEAHwgi2w)

Welcome to the open source project HiveRunner. HiveRunner is a unit test framework based on JUnit4 and enables TDD development of HiveQL without the need of any installed dependencies. All you need is to add HiveRunner to your pom.xml as any other library and you're good to go.

HiveRunner is under constant development. We use it extensively in all our Hive projects. Please feel free to suggest improvements both as Pull requests and as written requests.


A word from the inventors
---------
HiveRunner enables you to write Hive SQL as releasable tested artifacts. It will require you to parametrize and modularize HiveQL in order to make it testable. The bits and pieces of code should then be wired together with some orchestration/workflow/build tool of your choice, to be runnable in your environment (e.g. Oozie, pentaho, Talend, maven, etc…) 

So, even though your current Hive SQL probably won't run off the shelf within HiveRunner, we believe the enforced testability and enabling of a TDD workflow will do as much good to the scripting world of SQL as it has for the Java community.



Cook Book
==========

1. Include HiveRunner
----------

HiveRunner is published to [Maven Central](http://search.maven.org/). To start to use it, add a dependency to HiveRunner to your pom file.

    <dependency>
        <groupId>com.klarna</groupId>
        <artifactId>hiverunner</artifactId>
        <version>[HIVERUNNER VERSION]</version>
        <scope>test</scope>
    </dependency>

Alternatively, if you want to build from source, clone this repo and build with

     mvn install

Then add the dependency as mentioned above.

Also explicitly add the surefire plugin and configure forkMode=always to avoid OutOfMemory when building big test suites.

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.17</version>
        <configuration>
            <forkMode>always</forkMode>
        </configuration>
    </plugin>

as an alternative if this does not solve the OOM issues, try increase the -Xmx and -XX:MaxPermSize settings. For example:

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.17</version>
        <configuration>
            <forkCount>1</forkCount>
            <reuseForks>false</reuseForks>
            <argLine>-Xmx2048m -XX:MaxPermSize=512m</argLine>
        </configuration>
    </plugin>

(please note that the forkMode option is deprecated and you should use forkCount and reuseForks instead)

With forkCount and reuseForks there is a possibility to reduce the test execution time drastically, depending on your hardware. A plugin configuration which are using one
fork per CPU core and reuse threads would look like:

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.17</version>
        <configuration>
            <forkCount>1C</forkCount>
            <reuseForks>true</reuseForks>
            <argLine>-Xmx2048m -XX:MaxPermSize=512m</argLine>
        </configuration>
    </plugin>

By default, HiveRunner uses mapreduce (mr) as the execution engine for hive. If you wish to run using tez, set the 
System property hiveconf_hive.execution.engine to 'tez'.


(Any hive conf property may be overridden by prefixing it with 'hiveconf_')
        
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>2.17</version>
            <configuration>
                <systemProperties>
                    <hiveconf_hive.execution.engine>tez</hiveconf_hive.execution.engine>
                    <hiveconf_hive.exec.counters.pull.interval>1000</hiveconf_hive.exec.counters.pull.interval>
                </systemProperties>
            </configuration>
        </plugin>

Timeout - It's possible to configure HiveRunner to make tests time out after some time and retry those tests a couple of times.. This is to cover for the bug
https://issues.apache.org/jira/browse/TEZ-2475 that at times causes test cases to not terminate due to a lost DAG reference.
The timeout feature can be configured via the 'enableTimeout', 'timeoutSeconds' and 'timeoutRetries' properties.
A configuration which enables timeouts after 30 seconds and allows 2 retries would look like

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.17</version>
        <configuration>
            <systemProperties>
                <enableTimeout>true</enableTimeout>
                <timeoutSeconds>30</timeoutSeconds>
                <timeoutRetries>2</timeoutRetries>
            </systemProperties>
        </configuration>
    </plugin>


### Logging
src/main/resources/log4j.properties configures the log levels. Log level is default set to WARN. Some traces remain due to the fact that Hive logs to stdout.
All result sets are logged. Enable by setting ```log4j.logger.com.klarna.hiverunner.HiveServerContainer=DEBUG``` in log4j.properties.


2. Look at the examples
----------
Look at the [com.klarna.hiverunner.examples.HelloHiveRunner](/src/test/java/com/klarna/hiverunner/examples/HelloHiveRunner.java) reference test case to get a feeling for how a typical test case looks like. If you're put off by the verbosity of the annotations, there's always the possibility to use HiveShell in a more interactive mode.  The [com.klarna.hiverunner.SerdeTest](/src/test/java/com/klarna/hiverunner/SerdeTest.java) adds a resources (test data) interactively with HiveShell instead of using annotations.

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

See [com.klarna.hiverunner.examples.InsertTestData](/src/test/java/com/klarna/hiverunner/examples/InsertTestData.java) for working examples.

3. Understand a little bit of the order of execution
----------
HiveRunner will in default mode setup and start the HiveShell before the test method is invoked. If autostart is set to false, the [HiveShell](/src/main/java/com/klarna/hiverunner/HiveShell.java) must be started manually from within the test method. Either way, HiveRunner will do the following steps when start is invoked.

1. Merge any [@HiveProperties](/src/main/java/com/klarna/hiverunner/annotations/HiveProperties.java) from the test case with the hive conf
2. Start the HiveServer with the merged conf
3. Copy all [@HiveResource](/src/main/java/com/klarna/hiverunner/annotations/HiveResource.java) data into the temp file area for the test
4. Execute all fields annotated with [@HiveSetupScript](/src/main/java/com/klarna/hiverunner/annotations/HiveSetupScript.java)
5. Execute the script files given in the [@HiveSQL](/src/main/java/com/klarna/hiverunner/annotations/HiveSQL.java) annotation

The [HiveShell](/src/main/java/com/klarna/hiverunner/HiveShell.java) field annotated with [@HiveSQL](/src/main/java/com/klarna/hiverunner/annotations/HiveSQL.java) will always be injected before the test method is invoked.


Hive version compatibility
============
- This version of HiveRunner is built for hive 14.
- Command shell emulations are provided to closely match the behaviour of both the Hive CLI and Beeline interactive shells. The desired emulation can be specified in your `pom.xml` file like so: 

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>2.17</version>
            <configuration>
                <systemProperties>
                    <!-- Defaults to HIVE_CLI -->
                    <commandShellEmulation>BEELINE</commandShellEmulation>
                </systemProperties>
            </configuration>
        </plugin>

  Or provided on the command line using a system property:

      mvn -DcommandShellEmulation=BEELINE test

Future work and Limitations
============
* HiveRunner does not allow the add jar statement. It is considered bad practice to keep environment specific code together with the business logic that targets HiveRunner. Keep environment specific stuff in separate files and use your build/orchestration/workflow tool to run the right files in the right order in the right environment. When running HiveRunner, all SerDes available on the classpath of the IDE/maven will be available.

* HiveRunner runs Hive and Hive runs on top of hadoop, and hadoop has limited support for windows machines. Installing [Cygwin](http://www.cygwin.com/ "Cygwin") might help out.

* Some of the HiveRunner annotations should probably be rebuilt to be more test method specific. E.g. Resources may be described on a test method basis instead as for a whole test case. Feedback is always welcome!

* The interactive API of HiveShell should support as much as the annotated DSL. Currently some features are not implemented in the HiveShell but only available via annotations.

    __DONE:__ _HiveShell now includes all the features of the annotations and then some!_
 
* Clean up the logs
     
    __DONE:__ _There's now a working log4j.properties here: src/main/resources/log4j.properties_

* HiveRunner currently uses in-memory Derby as metastore. It seems to be a real performance bottleneck. We are looking to replace it with hsqldb in the near future.
   
    __DONE:__ _This version of HiveRunner uses an in-memory hsqldb instead of derby. It's faster and more reliant!_  

* Currently the HiveServer spins up and tears down for every test method. As a performance option it should be possible to clean the HiveServer and metastore between each test method invocation. The choice should probably be exposed to the test writer. By switching between different strategies, side effects/leakage can be ruled out during test case debugging.

* Redirect derby.log. It currently ends up in the build root dir.

    __DONE:__ _Derby is gone -> derby.log is gone!_ 


Change Log (From version 2.2.0 and onwards)
==============

### __3.2.1__
* The way of setting writable permissions on JUnit temporary folder changed to make it compatible with Windows.

### __3.2.0__
* Added functionality for headers in TSV parser. This way you can dynamically add TSV files declaring a subset of columns using insertInto.

### __3.1.1__
* Added debug logging of result set. Enable by setting ```log4j.logger.com.klarna.hiverunner.HiveServerContainer=DEBUG``` in log4j.properties.

### __3.1.0__
* Added methods to the shell that allow statements contained in files to be executed and their results gathered. These are particularly useful for HQL scripts that generate no table based data and instead write results to STDOUT. In practice we've seen these scripts used in data processing job orchestration scripts (e.g `bash`) to check for new data, calculate processing boundaries, etc. These values are then used to appropriately configure and launch some downstream job.
* Support abstract base class (Issue #48).

### __3.0.0__

* Upgraded to Hive 1.2.1 (Note: new major release with backwards incompatibility issues). As of Hive 1.2 there are a number of new reserved keywords, see [DDL manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Keywords,Non-reservedKeywordsandReservedKeywords) for more information. 
If you happen to have one of these as an identifier, you could either backtick quote them (e.g. \`date\`, \`timestamp\` or \`update\`) or set hive.support.sql11.reserved.keywords=false.                                            
* Removed the custom HiveConf hive.vs. Use hadoop.tmp.dir instead.
* Users of Hive version 0.14 or older are recommended to use HiveRunner version 2.6.0.

### __2.6.0__

* Introduced command shell emulations to replicate different handling of full line comments in `hive` and `beeline` shells.
Now strips full line comments for executed scripts to match the behaviour of the `hive -f` file option. 

* Option to use files as input for com.klarna.hiverunner.HiveShell.execute(...)

### __2.5.1__

Fixed  deadlock in ThrowOnTimeout.java that occured when running with long running test case and disabled timeout.

### __2.5.0__

Added support with `HiveShell.insertInto` for fluently generating test data in a table storage format agnostic manner.

### __2.4.0__

Enabled any hiveconf variables to be set as System properties by using the naming convention
hiveconf_[HiveConf property name]. E.g: hiveconf_hive.execution.engine

Fixed bug: Results sets bigger than 100 rows only returned the first 100 rows. 

### __2.3.0__

Merged tez and mr context into the same context again. Now, the same test suite may alter between execution engines by doing 
E.g: 

     hive> set hive.execution.engine=tez;
     hive> [some query]
     hive> set hive.execution.engine=mr;
     hive> [some query]


### __2.2.0__
* Added support for setting hivevar:s via HiveShell 




Known Issues
=====================

### UnknownHostException
I've had issues with UnknownHostException on OS X after upgrading my system or running docker. 
Usually a restart of my machine solved it, but last time I got some corporate 
stuff installed the restarts stopped working and I kept getting UnknownHostExceptions. 
Following this simple guide solved my problem:
http://crunchify.com/getting-java-net-unknownhostexception-nodename-nor-servname-provided-or-not-known-error-on-mac-os-x-update-your-privateetchosts-file/


### IOException in Hive 0.14.0
Described in this issue: https://github.com/klarna/HiveRunner/issues/3

This is a known bug in hive. Try setting hive.exec.counters.pull.interval to 1000 millis. It has worked for some projects.
You can do this in the surefire plugin:
 
          <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-surefire-plugin</artifactId>
              <version>2.17</version>
              <configuration>
                  <systemProperties>
                      <hiveconf_hive.exec.counters.pull.interval>1000</hiveconf_hive.exec.counters.pull.interval>
                  </systemProperties>
              </configuration>
          </plugin>
 
Also you can try to use the retry functionality in Surefire: https://maven.apache.org/surefire/maven-surefire-plugin/examples/rerun-failing-tests.html 


### Tez queries do not terminate
Tez will at times forget the process id of a random DAG. This will cause the query to never terminate. To get around this there is 
a timeout and retry functionality implemented in HiveRunner:
 
         <plugin>
             <groupId>org.apache.maven.plugins</groupId>
             <artifactId>maven-surefire-plugin</artifactId>
             <version>2.17</version>
             <configuration>
                 <systemProperties>
                     <enableTimeout>true</enableTimeout>
                     <timeoutSeconds>30</timeoutSeconds>
                     <timeoutRetries>2</timeoutRetries>
                     </systemProperties>
             </configuration>
         </plugin>
         
Make sure to set the timeoutSeconds to that of your slowest test in the test suite and then add some padding.

TAGS
=========
Hive Hadoop HiveRunner HDFS Unit test JUnit SQL HiveSQL HiveQL


Releasing hiverunner to maven central
=====================================

Deployment to Sonatype OSSRH using maven and travis-ci
------------------------------------------------------

HiveRunner has been setup to build continuously on a travis-ci.org build server as well as prepared to be manually released from a travis-ci.org buildserver to maven central.
The following steps were involved.

* A Sonatype OSSRH (Open Source Software Repository Hosting) account has been created for user "klarna.odin".
* The OSSRH username and password were encrypted according to http://docs.travis-ci.com/user/encryption-keys/
* A special maven settings.xml file was created that uses the encrypted environment variables in the ossrh server definition.
* A Gnu PGP keypair was created locally according to http://central.sonatype.org/pages/working-with-pgp-signatures.html
* The pubring.gpg and secring.gpg were tar'ed into secrets.tar
* The secrets.tar was encrypted according to http://docs.travis-ci.com/user/encrypting-files/
* The GPG_PASSPHRASE variable was encrypted for travis usage.
* The pom has had sections added to it according to instructions from http://central.sonatype.org/pages/apache-maven.html
* A .travis.yml build file has been added

The above steps are enough for deploying to sonatype/maven central.
Depending on the version number in the pom, the build artifact will be deployed to either the snapshots repository or the staging-repository.

Note
----
The gpg key used for signing expires 2017-10-17, after which a new one needs to be created and added as described above.
Don't forget that the GPG_PASSPHRASE also needs to be updated if another passphrase is used when creating the gpg keypair.


Playbook for making a release
-----------------------------
Basically follow this guide: http://central.sonatype.org/pages/apache-maven.html#performing-a-release-deployment

* Change the version number to the release version you want. Should not include -SNAPSHOT in the name.
* Update change log for new version.
* Commit, tag with release number, push

```
      git commit -a -m "Setting version number before releasing"
      git tag -a v2.5.0 -m "HiveRunner-2.5.0"
      git push origin --tags
```

* Travis builds and deploys. Make sure to check the status of the build in Travis (https://travis-ci.org/klarna/HiveRunner).
* Follow the http://central.sonatype.org/pages/releasing-the-deployment.html guide to promote the staged release to maven central.
* Change version number to next snapshot version
* Commit and push

```
     git commit -m "Setting version to next development version"
     git push origin
```
 
