#!/bin/bash

# Crappy script to sync directories from this repository
# that are useful to have in HDFS.
# TODO: turn this into a cooooool git hook.
#
# Usage:
# ./hdfs_sync.sh [dest=/libs/kraken]

dest="/libs/kraken"
if [ -n "${1}" ]; then
    dest="${1}"
fi

kraken_path=$(cd "$(dirname "$0")/.."; pwd)

to_sync="oozie pig"

for d in $to_sync; do 
    hadoop fs -mkdir -p $dest
    c="hadoop fs -rm -r -f $dest/$d && hadoop fs -put $kraken_path/$d $dest/"
    echo $c;
done
