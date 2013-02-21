BASE_URL="https://repository.cloudera.com/artifactory/cloudera-repos/org/apache";
CLOUDERA_VERSION="cdh4.1.2";
PIG_VERSION="0.10.0";
HADOOP_COMMON="";


wget "$BASE_URL/pig/pigunit/$PIG_VERSION-$CLOUDERA_VERSION/pigunit-$PIG_VERSION-$CLOUDERA_VERSION.jar";
wget "$BASE_URL/pig/pig/$PIG_VERSION-$CLOUDERA_VERSION/pig-$PIG_VERSION-$CLOUDERA_VERSION.jar";

mvn install:install-file -DgroupId=org.apache.pig -DartifactId=pig -Dversion=$PIG_VERSION-$CLOUDERA_VERSION -Dpackaging=jar -Dfile=pig-$PIG_VERSION-$CLOUDERA_VERSION.jar
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=pig -Dversion=$PIG_VERSION-$CLOUDERA_VERSION -Dpackaging=jar -Dfile=pigunit-$PIG_VERSION-$CLOUDERA_VERSION.jar
