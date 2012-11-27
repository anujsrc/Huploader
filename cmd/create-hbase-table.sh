#!/bin/bash
# This is to preprocess the files to CSV files
#input: file format(xml,binary), input directory in HDFS/local storage, output directory
#output: CSV files are stored in output directory


# use the command to upload the data into HDFS
USAGE="<input directory> <format 0:bixi 1: cosmology> <template file name> <output directory>"
if [ -z "$1" ]; then
	echo "$USAGE"
	exit -1
fi

if [ -z "$2" ]; then
	echo "$USAGE"
	exit -1
fi

if [ -z "${HADOOP_HOME}" ]; then
  	echo "Please set the HADOOP_HOME first"
	exit -1
fi


#myDir=$(readlink -f $0 | xargs dirname)

MYLIB=${PWD}/../bin/huploader.jar
MYCONF=${PWD}/../conf/

# jar file which is used to preprocess the files

${JAVA_HOME}/bin/java -Xmx1500m -classpath ${MYLIB}:${MYCONF} com.preprocessor.Transformer $* 

