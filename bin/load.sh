#!/bin/bash

# Wrapper script for running cascading load benchmarks
# http://www.cascading.org/load/

now=$(date "+%Y-%m-%d_%H.%M.%S")

load_jar="/home/otto/cascading/load/load-20120605.jar"
hadoop_input_path="/user/otto/load/input"
hadoop_output_path="/user/otto/load/output"
hadoop_status_path="/user/otto/load/status"
block_size=67108864    # 64MB
default_mappers=144
default_reducers=72
file_size=1073741824  # 1GB
num_files=1024        # 1024 * 1GB = 1TB
log_file="./cascading_load.$now.log"


usage="
Wrapper script for Cascading load tests.  See: http://www.cascading.org/load/

Usage:
$0
  -j <path_to_load_jar>          Default: $load_jar
  -b <block_size>                Default: $block_size
  -I <hadoop_input_path>         Default: $hadoop_input_path
  -O <hadoop_output_path>        Default: $hadoop_output_path
  -S <hadoop_status_path>        Default: $hadoop_status_path
  -m <default_num_mappers>       Default: $default_mappers
  -r <default_num_reducers>      Default: $default_reducers
  -s <file_size>                 Default: $file_size
  -n <num_files>                 Default: $num_files
  -l <log_file>                  Default: $log_file
"


while getopts "j:I:O:S:b:m:r:s:n:l:h" OPTION; do
  case $OPTION in
    j) load_jar=$OPTARG ;;
    I) hadoop_input_path=$OPTARG ;;
    O) hadoop_output_path=$OPTARG ;;
    S) hadoop_status_path=$OPTARG ;;
    b) block_size=$OPTARG ;;
    m) default_mappers=$OPTARG ;;
    r) default_reducers=$OPTARG ;;
    s) file_size=$OPTARG ;;
    n) num_files=$OPTARG ;;
    l) log_ile=$OPTARG ;;
    h) echo "${usage}"; exit 0 ;;
    \?) echo "${usage}"; exit 1 ;;
  esac
done


command="hadoop jar $load_jar \
-I $hadoop_input_path \
-O $hadoop_output_path \
-S $hadoop_status_path \
--BS $block_size \
--NM $default_mappers \
--NR $default_reducers \
--gs $file_size \
--gf $num_files \
--ALL"


echo "Running
  $command 
at $now, logging stdout and stderr to $log_file" | tee -a $log_file
time $command 2>&1 | tee -a $log_file

