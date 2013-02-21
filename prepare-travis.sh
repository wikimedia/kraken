BASE_URL="https://repository.cloudera.com/artifactory/cloudera-repos/org/apache";
CLOUDERA_VERSION="cdh4.1.2";
PIG_VERSION="0.10.0-$CLOUDERA_VERSION";
HADOOP_VERSION="2.0.0-$CLOUDERA_VERSION";


wget "$BASE_URL/hadoop/hadoop-minicluster/$HADOOP_VERSION/hadoop-minicluster-$HADOOP_VERSION.jar";
wget "$BASE_URL/hadoop/hadoop-hdfs/$HADOOP_VERSION/hadoop-hdfs-$HADOOP_VERSION.jar";
wget "$BASE_URL/hadoop/hadoop-client/$HADOOP_VERSION/hadoop-client-$HADOOP_VERSION.jar";
wget "$BASE_URL/hadoop/hadoop-common/$HADOOP_VERSION/hadoop-common-$HADOOP_VERSION.jar";
wget "$BASE_URL/pig/pigunit/$PIG_VERSION/pigunit-$PIG_VERSION.jar";
wget "$BASE_URL/pig/pig/$PIG_VERSION/pig-$PIG_VERSION.jar";

mvn install:install-file -DgroupId=org.apache.pig -DartifactId=hadoop-common -Dversion=$HADOOP_VERSION -Dpackaging=jar -Dfile=hadoop-common-$HADOOP_VERSION.jar
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=hadoop-client -Dversion=$HADOOP_VERSION -Dpackaging=jar -Dfile=hadoop-client-$HADOOP_VERSION.jar
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=hadoop-hdfs -Dversion=$HADOOP_VERSION -Dpackaging=jar -Dfile=hadoop-hdfs-$HADOOP_VERSION.jar
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=hadoop-minicluster -Dversion=$HADOOP_VERSION -Dpackaging=jar -Dfile=hadoop-minicluster-$HADOOP_VERSION.jar

#Install pig jars
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=pig -Dversion=$PIG_VERSION -Dpackaging=jar -Dfile=pig-$PIG_VERSION.jar
mvn install:install-file -DgroupId=org.apache.pig -DartifactId=pigunit -Dversion=$PIG_VERSION -Dpackaging=jar -Dfile=pigunit-$PIG_VERSION.jar
