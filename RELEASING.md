# Releasing HiveRunner to Maven Central

HiveRunner has been set up to build continuously and also to deploy SNAPSHOTS so Sonatype and releases to Maven Central via GitHub Actions.

## Deploying a SNAPSHOT to Sonatype

* Select the https://github.com/HiveRunner/HiveRunner/actions/workflows/deploy.yml worfklow
* Select the branch to use to deploy a SNAPSHOT from:
** Use `main` as the branch for a Hive 3.x release
** Use `hive-2.x` as the branch for a Hive 2.x release
* Run the workflow
* SNAPSHOT artifacts will be available at https://s01.oss.sonatype.org/content/repositories/snapshots/io/github/hiverunner/hiverunner/

## Deploying a release to Maven Central

* Ensure the `pom.xml` has the SNAPSHOT version set to the value you would like to make a release from
* Update `CHANGELOG.md` with the date corresponding to when you're performing the release
* Select the https://github.com/HiveRunner/HiveRunner/actions/workflows/release.yml worfklow
* Select the branch to use to deploy a SNAPSHOT from:
** Use `main` as the branch for a Hive 3.x release
** Use `hive-2.x` as the branch for a Hive 2.x release
* Run the workflow
* Release artifacts will be available at https://repo1.maven.org/maven2/io/github/hiverunner/hiverunner/
* It can take a few hours before the artifacts show up in searches performed at https://search.maven.org/search?q=hiverunner
