#!/bin/bash
# This is to preprocess the files to CSV files
#input: file format(xml,binary), input directory in HDFS/local storage, output directory
#output: CSV files are stored in output directory


# use the command to upload the data into HDFS
USAGE="<distribution type "uniform" or "zipf"> <number of points> <min value> <max value> <output folder>"
if [ -z "$1" ]; then
	echo "$USAGE"
	exit -1
fi

if [ -z "$2" ]; then
	echo "$USAGE"
	exit -1
fi

#myDir=$(readlink -f $0 | xargs dirname)

MYLIB=${PWD}/../bin/huploader.jar:\
${PWD}/../lib/commons-math3-3.0.jar
MYCONF=${PWD}/../conf/

# jar file which is used to preprocess the files
# java heap space should be large, because it will generate large data
${JAVA_HOME}/bin/java -Xms1500m -Xmx1500m -classpath ${MYLIB}:${MYCONF} com.benchmark.dataset.XBixiGenerator $* 

