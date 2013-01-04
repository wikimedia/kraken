# Installing
You need Maven to build and package the Kraken User Defined Functions for Pig. At the very minimum, you would issue the following commands:
mvn clean
mvn package
mvn site-deploy

The latter command expects that you configured Maven properly to deploy the javadocs and findbugs reports to a server.
