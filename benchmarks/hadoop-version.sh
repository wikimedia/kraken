#!/bin/bash

# TODO: Make this dynamic from `hadoop version`

HADOOP_VERSION=$(hadoop version | sed -n '1p' | awk '{print $2}')
JAVA_VERSION=$(java -version 2>&1 | cat)

# export cdh_version=4.3.1
# export hadoop_version=2.0.0