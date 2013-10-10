#!/bin/bash
#
# This script does the following:
#       0. reads four arguments from the CLI, in order, as YEAR, MONTH, DAY, HOUR
#       1. downloads the specified hour worth of data from http://dumps.wikimedia.org/other/pagecounts-raw/
#       2. extracts the data into hdfs
#       3. creates a partition on a hive table pointing to this data
#

print_help() {
        cat <<EOF
        
USAGE: import_one_hour.sh YEAR MONTH DAY HOUR
        
  Imports one hour worth of data from dumps.wikimedia.org/other/pagecounts-raw into a Hive table

EOF
}

if [ $# -ne 4 ]
then
        print_help
        exit
fi

YEAR=$1
MONTH=$2
DAY=$3
HOUR=$4

# Someone intelligent should set these
LOCAL_FILE=pagecounts-$YEAR$MONTH$DAY-${HOUR}0000.gz
HDFS_DIR=/user/milimetric/pagecounts/$YEAR.$MONTH.${DAY}_${HOUR}.00.00
HDFS_FILE=pagecounts-$YEAR$MONTH$DAY-${HOUR}0000
TABLE=milimetric_pagecounts

rm $LOCAL_FILE
wget http://dumps.wikimedia.org/other/pagecounts-raw/$YEAR/$YEAR-$MONTH/pagecounts-$YEAR$MONTH$DAY-${HOUR}0000.gz
hdfs dfs -rm -r $HDFS_DIR
gunzip -c $LOCAL_FILE | hdfs dfs -put - $HDFS_DIR/$HDFS_FILE
rm $LOCAL_FILE
hive -e "ALTER TABLE ${TABLE} DROP PARTITION (year='$YEAR',month='$MONTH',day='$DAY',hour='$HOUR');"
hive -e "ALTER TABLE ${TABLE} ADD PARTITION (year='$YEAR',month='$MONTH',day='$DAY',hour='$HOUR') location '$HDFS_DIR';"
