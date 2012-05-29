Installation Tasks

## Env Setup

setup.sh will roughly do the following for every box in the cluster...

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
    - nginx
    - zmq, jzmq
- Packages to support jobs
    - python 2.7, pip, virtualenv
    - node, npm
    - ruby, gem



## Cluster Software

Userenv-to-software mappings.

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
    - Kestrel?
- `zk`: Zookeeper
- `etl`: Storm



## Utility Box

- nginx
- Gitosis with configuration files (as above) and keys
- Monitoring software: I'd like to research this -- I've used nagios and zabbix before, and both are pretty much ass.








