# lists kafka broker topics from zookeeper
# and consumes each of them that start with "event"
# into hadoop.


hdfs_event_log_path=$1
: ${hdfs_event_log_path:="/user/otto/event/logs"}

cd /home/otto/kafka-hadoop-consumer/
kakfa_hadoop_consume=./kafka-hadoop-consume
hadoop fs -mkdir -p $hdfs_event_log_path

# consume all topics that start with "event" into $hdfs_event_log_path/$topic/$timestamp
for topic in $(zookeeper-client -server analytics1023.eqiad.wmnet:2181 ls /brokers/topics | tail -n 1 | tr '[],' '   '); do 
    if [[ $topic == event* ]]; then
        log_path=$hdfs_event_log_path/$topic
        timestamp=$(date "+%Y-%m-%d_%H.%M.%S")
        hadoop fs -mkdir -p $log_path
        echo "Consuming $topic logs into $log_path/$(date "+%Y-%m-%d_%H.%M.%S")"
        $kakfa_hadoop_consume --topic=$topic --group=kconsumer0 --limit=-1 --output=$log_path/$(date "+%Y-%m-%d_%H.%M.%S")
    fi
done