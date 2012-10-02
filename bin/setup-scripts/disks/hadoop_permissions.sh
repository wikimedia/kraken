#!/bin/bash

# Sets proper permissions and ownership.

hadoop_data_directory="/var/lib/hadoop/data"
hadoop_name_directory="/var/lib/hadoop/name"


if [ -d $hadoop_data_directory ]; then 
    sudo chgrp hadoop $hadoop_data_directory
    for disk_letter in e f g h i j ; do 
        data_directory="$hadoop_data_directory/${disk_letter}"
        sudo chgrp hadoop $data_directory
        sudo chown hdfs:hdfs $data_directory/{hdfs,hdfs/dn}
        sudo chmod 700 $data_directory/{hdfs,hdfs/dn}
        sudo chown yarn:yarn $data_directory/{yarn,yarn/{local,logs}}
        sudo chmod 755 $data_directory/{yarn,yarn/{local,logs}}
    done
fi

if [ -d $hadoop_name_directory ]; then
    sudo chown hdfs:hdfs $hadoop_name_directory && sudo chmod 700 $hadoop_name_directory
fi