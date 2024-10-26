# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [7.0.0] - TBD
### Changed
- Updated `hadoop-mapreduce-client-common` from `3.1.0` to `3.4.1`.
- Updated `hadoop-mapreduce-client-core` from `3.1.0` to `3.4.1`.
- Updated `hadoop-client-runtime` from `3.1.0` to `3.4.1`.
- Updated `hive-exec` from `3.1.2` to `4.0.1`.
- Updated `hive-serde` from `3.1.0` to `3.4.1`.
- Updated `hive-jdbc` from `3.1.0` to `3.4.1`.
- Updated `hive-contrib` from `3.1.0` to `3.4.1`.
- Updated `hive-webhcat-java-client` from `3.1.0` to `3.4.1`.
- Updated `jackson-annotations` from `2.9.5` to `2.18.0`.
- Updated `reflections` from `0.9.8` to `0.10.2`.
- Updated `mockito-core` from `3.8.0` to `5.14.2`.
- Updated `mockito-junit-jupiter` from `3.8.0` to `5.14.2`.
- Updated `tez-common` from `0.9.1` to `0.10.4`.
- Updated `tez-mapreduce` from `0.9.1` to `0.10.4`.
- Updated `junit-jupiter` from `5.7.1` to `5.11.2`.
- Updated `junit-vintage-engine` from `5.7.1` to `5.11.2`.
- Added version `6.0.8` of `datanucleus-core`.
- Added version `6.0.3` of `datanucleus-api-jdo`.
- Added version `6.0.8` of `datanucleus-rdbms`.
- Added version `1.3` of `javax.transaction-api`.
- Added version `6.1.14` of `spring-jdbc`.
- Added version `10.15.2.0` of `derby`.
- Added version `10.15.2.0` of `derbytools`.
- Added version `5.6.2` of `kryo`.
- Added version `4.9.3` of `antlr4-runtime`.
- Updated `maven-surefire-plugin` from `2.22.2` to `3.5.1`.
- Updated `maven-compiler-plugin` from `3.7.0` to `3.13.0`.
- Updated `maven-jar-plugin` from `3.2.0` to `3.4.2`.
- Updated `maven-release-plugin` from `3.0.0-M1` to `3.1.1`.
- Updated `nexus-staging-maven-plugin` from `1.6.8` to `1.7.0`.
- Updated `maven-source-plugin` from `3.2.0` to `3.3.1`.
- Updated `maven-javadoc-plugin` from `3.2.0` to `3.10.1`.
- Updated `maven-gpg-plugin` from `1.6` to `3.2.7`.
- Removed com.google.common.base.Predicates in HiveRunnerExtension/StandaloneHiveRunner as it is no longer using in a new version of reflections library
- Updated HiveConf property names in StandaloneHiveServerContext
- Set METASTORE_VALIDATE_CONSTRAINTS, METASTORE_VALIDATE_COLUMNS, METASTORE_VALIDATE_TABLES properties to false in StandaloneHiveServerContext
- Added missing Hive & Datanucleus properties in StandaloneHiveServerContext so now the framework works on a new hive dependency versions

## [6.1.0] - 2021-04-28
### Changed
- Maven Group Id changed from `com.klarna` to `io.github.hiverunner`.
- Set `HIVE_IN_TEST` to true in `StandaloneHiverServerContext` instead of `StandaloneHiveRunner` so checks for non-existent tables are skipped by both the JUnit4 runner and the JUnit5 extension (this removes a lot of log noise from tests using the latter).
- Made `HiveRunnerScript` constructor public.
- Made `scriptsUnderTest` variable in `HiveRunnerExtension` protected so it can be used in [MutantSwarm](https://github.com/HotelsDotCom/mutant-swarm).
- Fixed bug that appears in [Mutant Swarm](https://github.com/HotelsDotCom/mutant-swarm) when updating HiveRunner to version 5.2.1.
- Renamed `HelloAnnotatedHiveRunner` in `com.klarna.hiverunner.examples` to `HelloAnnotatedHiveRunnerTest`.
- Renamed `HelloHiveRunner` in `com.klarna.hiverunner.examples` to `HelloHiveRunnerTest`.
- Renamed `InsertTestData` in `com.klarna.hiverunner.examples` to `InsertTestDataTest`.
- Renamed `SetHiveConfValues` in `com.klarna.hiverunner.examples` to `SetHiveConfValuesTest`.
- Renamed `HelloAnnotatedHiveRunner` in `com.klarna.hiverunner.examples.junit4` to `HelloAnnotatedHiveRunnerTest`.
- Renamed `HelloHiveRunner` in `com.klarna.hiverunner.examples.junit4` to `HelloHiveRunnerTest`.
- Renamed `InsertTestData` in `com.klarna.hiverunner.examples.junit4` to `InsertTestDataTest`.
- Renamed `SetHiveConfValues` in `com.klarna.hiverunner.examples.junit4` to `SetHiveConfValuesTest`.
- Updated `surefire-version-plugin` from `2.21.0` to `2.22.0`.
- Updated `junit.jupiter.version` (JUnit5) from `5.6.0` to `5.7.1`.
- Updated `junit` (JUnit4) from `4.13.1` to `4.13.2`.
- Updated `mockito-core` from `2.18.3` to `3.8.0`.

### Added
- Added `getScriptPaths` method in `HiveRunnerCore`.
- Added `getScriptPaths` method in `HiveRunnerExtension` to be able to access the other method in `HiveRunnerCore` so that it can be used downstream in [MutantSwarm](https://github.com/HotelsDotCom/mutant-swarm).
- Added `fromScriptPaths` method in `HiveShellBuilder`.
- Added version `5.7.1` of `junit-vintage-engine`.
- Added version `3.8.0` of `mockito-junit-jupiter`.

### Fixed
- Fixed bug where the files specified in `@HiveSQL` weren't being run when using `HiveRunnerExtension`.
- Successful tests using "SET" no longer marked as "terminated" when run in IntelliJ. See [#94](https://github.com/klarna/HiveRunner/issues/94).

## [6.0.1] - 2020-09-07
### Removed
- Removed shaded jar that was being produced as a side-effect.

## [6.0.0] - 2020-09-03
### Changed
- Upgraded Hive version to 3.1.2 (was 2.3.7).

## [5.x]
### NOTE
- Releases from the 5.x (Hive 2) line are not tracked in this CHANGELOG, it only tracks 6.0.0 and above. For changes in 5.x please refer to https://github.com/klarna/HiveRunner/blob/hive-2.x/CHANGELOG.md.

## [5.0.0] - 2019-09-30
### Added
- JUnit5 [Extension](https://junit.org/junit5/docs/current/user-guide/#extensions) support with `HiveRunnerExtension`. See [#106](https://github.com/klarna/HiveRunner/issues/106).

### Changed
- Default supported Hive version is now 2.3.4 (was 2.3.3) as version 2.3.3 has a [vulnerability](https://nvd.nist.gov/vuln/detail/CVE-2018-1314).
- `TemporaryFolder` ([JUnit 4](https://junit.org/junit4/javadoc/4.12/org/junit/rules/TemporaryFolder.html)) has been changed to `Path` ([Java NIO](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Path.html)) throughout the project for the JUnit5 update. 
- NOTE: The `HiveServerContext` class now uses `Path` instead of `TemporaryFolder` in the constructor.

## [4.1.0] - 2019-02-27
### Changed
- Internal refactoring to support upcoming "Mutant Swarm" project which provides unit test coverage for Hive SQL scripts. See [#65](https://github.com/klarna/HiveRunner/issues/65).

## [4.0.0] - 2018-07-17
### Added
- Support shell-specific `source` (`hive`) and ``!run`` (`beeline`) commands. These commands allow one to import and execute the contents of external files in statements or scripts.

### Changed
- Default supported Hive version is now 2.3.3 (was 1.2.1).
- Default supported Tez version is now 0.9.1 (was 0.7.0).
- Supported Java version is 8 (was 7).
- In-memory DB used by HiveRunner is now Derby (was HSQLDB).
- Log4J configuration file removed from jar artifact.
- System property to configure command shell emulation mode renamed to `commandShellEmulator` (was `commandShellEmulation`).

## [3.2.1] - 2018-05-31
### Changed
- Fixed issue where if case of column name in a file was different to case in table definition they would be treated as different [#73](https://github.com/klarna/HiveRunner/issues/73).
- The way of setting writable permissions on JUnit temporary folder changed to make it compatible with Windows [#63](https://github.com/klarna/HiveRunner/issues/63).

## [3.2.0] - 2017-02-09
### Added
- Added functionality for headers in TSV parser. This way you can dynamically add TSV files declaring a subset of columns using insertInto.

## [3.1.1] - 2017-01-27
### Added
- Added debug logging of result set. Enable by setting ```log4j.logger.com.klarna.hiverunner.HiveServerContainer=DEBUG``` in log4j.properties.

## [3.1.0] - 2016-10-17
### Added
- Added methods to the shell that allow statements contained in files to be executed and their results gathered. These are particularly useful for HQL scripts that generate no table based data and instead write results to STDOUT. In practice we've seen these scripts used in data processing job orchestration scripts (e.g `bash`) to check for new data, calculate processing boundaries, etc. These values are then used to appropriately configure and launch some downstream job.
- Support abstract base class [#48](https://github.com/klarna/HiveRunner/issues/48).

## [3.0.0] - 2016-02-05
### Changed
- Upgraded to Hive 1.2.1 (Note: new major release with backwards incompatibility issues). As of Hive 1.2 there are a number of new reserved keywords, see [DDL manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Keywords,Non-reservedKeywordsandReservedKeywords) for more information. If you happen to have one of these as an identifier, you could either backtick quote them (e.g. \`date\`, \`timestamp\` or \`update\`) or set hive.support.sql11.reserved.keywords=false.                                            
- Users of Hive version 0.14 or older are recommended to use HiveRunner version 2.6.0.
- Removed the custom HiveConf hive.vs. Use hadoop.tmp.dir instead.

## [2.6.0] - 2015-12-01
### Added
- Introduced command shell emulations to replicate different handling of full line comments in `hive` and `beeline` shells. Now strips full line comments for executed scripts to match the behaviour of the `hive -f` file option. 
- Option to use files as input for com.klarna.hiverunner.HiveShell.execute(...).

## [2.5.1] - 2015-11-12
### Changed
- Fixed deadlock in `ThrowOnTimeout.java` that occurred when running with long running test case and disabled timeout.

## [2.5.0]
### Added
- Added support with `HiveShell.insertInto` for fluently generating test data in a table storage format agnostic manner.

## [2.4.0]
### Changed
- Enabled any hiveconf variables to be set as System properties by using the naming convention hiveconf_[HiveConf property name]. e.g: hiveconf_hive.execution.engine.
- Fixed bug: Results sets bigger than 100 rows only returned the first 100 rows. 

## [2.3.0]
### Changed
- Merged Tez and MR context into the same context again. Now, the same test suite may alter between execution engines by doing e.g.: 

     hive> set hive.execution.engine=tez;
     hive> [some query]
     hive> set hive.execution.engine=mr;
     hive> [some query]

## [2.2.0]
### Added
- Added support for setting hivevars via HiveShell.
