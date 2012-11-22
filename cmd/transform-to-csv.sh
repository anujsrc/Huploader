#!/bin/bash
# This is to preprocess the files to CSV files
#input: file format(xml,binary), input directory in HDFS/local storage, output directory
#output: CSV files are stored in output directory


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



# jar file which is used to preprocess the files



