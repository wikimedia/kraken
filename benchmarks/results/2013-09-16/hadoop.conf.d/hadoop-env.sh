# Note: This file is managed by Puppet.

# Use YARN for all hadoop commands
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce

# Enable NameNode JMX connections on port 9980
HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote.port=9980 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"

# Enable DateNode JMX connections on port 9981
HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote.port=9981 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
