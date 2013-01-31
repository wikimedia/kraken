# Installing
You need Maven to build and package the Kraken User Defined Functions for Pig. At the very minimum, you would issue the following commands:
* mvn clean
* mvn package
## Deploy to Nexus repo
* mvn release:prepare -DdryRun (make sure there are no errors)
* mvn release:clean release:prepare 
* mvn release:perform

## Deploy documentation and findbug reports
(only necessary if you do not release a version of the jar to the Nexus repo)
* mvn javadoc:javadoc
* mvn site-deploy

The latter command expects that you configured Maven properly to deploy the javadocs and findbugs reports to a server.

For configuring your Maven installation, look at https://github.com/wikimedia/kraken/blob/master/maven/example.settings.xml
