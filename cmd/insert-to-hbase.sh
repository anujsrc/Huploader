#!/bin/bash
# This is to preprocess the files to CSV files
#input: file format(xml,binary), input directory in HDFS/local storage, output directory
#output: CSV files are stored in output directory


# use the command to upload the data into HDFS
USAGE="<csv_desc_file> <schema_desc_file> <hbase_conf_folder> <input_data_dir> <batchNum>"
if [ -z "$1" ] && [ -z "$2" ] && [ -z "$3" ] && [ -z "$4" ]; then
	echo "$USAGE"
	exit -1
fi

if [ ! -f "${JAVA_HOME}/bin/java" ]; then
	echo "JAVA_HOME not found."
	exit -1
fi

if [ ! -f "${HBASE_HOME}/hbase-0.94.1-security.jar" ]; then
        echo "HBASE_HOME not found, hbase-0.94.1 is needed."
        exit -1
fi

if [ ! -f "${HADOOP_HOME}/hadoop-core-1.0.3.jar" ]; then
        echo "HADOOP_HOME not found, hadoop-1.0.3 is needed."
        exit -1
fi


COMMONLIB=${HBASE_HOME}/lib/commons-lang-2.5.jar:\
${HBASE_HOME}/lib/commons-configuration-1.6.jar:\
${HBASE_HOME}/lib/log4j-1.2.16.jar:\
${HBASE_HOME}/lib/commons-logging-1.1.1.jar:\
${HBASE_HOME}/lib/zookeeper-3.4.3.jar:\
${HBASE_HOME}/lib/slf4j-api-1.4.3.jar:\
${HBASE_HOME}/lib/slf4j-log4j12-1.4.3.jar:\
${HBASE_HOME}/lib/protobuf-java-2.4.0a.jar:\
${HBASE_HOME}/lib/commons-cli-1.2.jar:\
${PWD}/../lib/json-simple-1.1.1.jar

HBASELIB=${HBASE_HOME}/hbase-0.94.1-security.jar
HBASECONF=${HBASE_HOME}/conf

HADOOPLIB=${HADOOP_HOME}/hadoop-core-1.0.3.jar
HADOOPCONF=${HADOOP_HOME}/conf

MAPRLIB=${HADOOP_HOME}/lib/jackson-core-asl-1.8.8.jar:\
${HADOOP_HOME}/lib/jackson-mapper-asl-1.8.8.jar


MYLIB=${PWD}/../bin/huploader.jar
MYCONF=${PWD}/../conf/

${JAVA_HOME}/bin/java -Xmx1500m -classpath ${COMMONLIB}:${HADOOPLIB}:${HADOOPCONF}:${HBASELIB}:${HBASECONF}:${MAPRLIB}:${MYLIB}:${MYCONF}   com.hbase.insert.csv.CSVDataUploader $*

