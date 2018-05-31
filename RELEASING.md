# Releasing HiveRunner to Maven Central

## Deployment to Sonatype OSSRH using Maven and Travis CI

HiveRunner has been setup to build continuously on a travis-ci.org build server as well as prepared to be manually released from a travis-ci.org buildserver to Maven central.
The following steps were involved.

* A Sonatype OSSRH (Open Source Software Repository Hosting) account has been created for user "klarna.odin".
* The OSSRH username and password were encrypted according to http://docs.travis-ci.com/user/encryption-keys/
* A special maven settings.xml (settings-4-travis.xml) file was created that uses the encrypted environment variables in the ossrh server definition.
* A GNU PGP keypair was created locally according to http://central.sonatype.org/pages/working-with-pgp-signatures.html
* The pubring.gpg and secring.gpg were tar'ed into secrets.tar
* The secrets.tar was encrypted according to http://docs.travis-ci.com/user/encrypting-files/
* The GPG_PASSPHRASE variable was encrypted for Travis usage.
* The pom has had sections added to it according to instructions from http://central.sonatype.org/pages/apache-maven.html
* A .travis.yml build file has been added

Depending on the version number in the pom, the build artifact will be deployed to either the snapshots repository or the releases repository.

## Playbook for making a release

Basically follow this guide: http://central.sonatype.org/pages/apache-maven.html#performing-a-release-deployment

* Change the version number in the pom to the release version you want. Should not include -SNAPSHOT in the name.
* Update CHANGELOG.md for this new version.
* Commit, tag with release number, push:

```
      git commit -a -m "Setting version number before releasing"
      git tag -a v3.2.1 -m "HiveRunner-3.2.1"
      git push origin --tags
```

* Travis builds and deploys. Make sure to check the status of the build in Travis (https://travis-ci.org/klarna/HiveRunner).
* Check that the artifacts are now available in Maven Central.
* Change version number in pom to next SNAPSHOT version.
* Commit and push:

```
     git commit -a -m "Setting version to next development version"
     git push origin
```
