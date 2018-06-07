# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [TBA]
### Added
- Support shell-specific `source` (`hive`) and ``!run`` (`beeline`) commands. (These commands allow to import and execute scripts in statements or scripts.)

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
