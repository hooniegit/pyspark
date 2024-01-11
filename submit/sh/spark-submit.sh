#!/bin/bash

# set SPARK_HOME to use this file
# run command: ./spark-submit.sh <spark_dir>

SPARK_FILE="$1"
$SPARK_HOME/bin/spark-submit \
$SPARK_FILE \
&