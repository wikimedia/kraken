# Installation Tasks


- Users
    - Create Users: `cass`, `etl`, `zk`, `cdh4`, `cdh3`, `bundler`
    - Create Group: `kraken`
    - Add users to group `kraken`
    - Create ssh keys for each user
    - Aggregate & propagate `.ssh/authorized_keys`
    - Push keys into repo
- Install deps & tools (all boxes):
    - build-essentials
    - java (sun jdk), jni, maven, ant
    - git
    - python 2.7, pip, virtualenv
    - nginx
    - node, npm
    - zmq, jzmq
- Utility Box:
    - Gitosis?
- Cluster Software:
    - `cass`
        - Cassandra
        - DataStax Enterprise
    - `cdh4`
        - CDH4
        - Hive
        - Pig
    - `cdh3`
        - CDH3
        - Hive
        - Pig
    - `bundler`
        - Scribe
        - Flume
        - Kestrel
    - `zk`: Zookeeper
    - `etl`: Storm

