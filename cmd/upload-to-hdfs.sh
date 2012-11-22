#!/bin/bash
#input: the location of files
#output: files are copied HDFS

# use the command to upload the data into HDFS
USAGE="<local directory> <output directory in HDFS> ** check the output folder exist or not first"
if [ -z "$1" ]; then
	echo "$USAGE"
	exit -1
fi

if [ -z "$2" ]; then
	echo "$USAGE"
	exit -1
fi

if [ -z "${HADOOP_HOME}"]; then
  	echo "Please set the HADOOP_HOME first"
	exit -1
fi

echo "Delete the output directory in HDFS already"
hadoop fs -rmr $2

echo "start to upload the files in $1 to $2"

hadoop fs -put $1 $2
