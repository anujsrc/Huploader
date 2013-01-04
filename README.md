Huploader
=========

This project is to faciliate people upload raw data to HBase easily.

There are many scripts to help you process your raw data.
So the steps are as followed:

1) preprocess raw data to csv format

  preprocess.sh

2) upload the processed data into HDFS

  upload-to-hdfs.sh

3) create data schema in hbase

   create-data-schema.sh

4) execute mapreduce to insert data into hbase

   execute-map-reduce.sh


Development 
=========

Dependencies:
commons-configuration-1.6.jar
commons-httpclient-3.1.jar
commons-lang-2.5.jar
commons-logging-1.1.1.jar
hadoop-core-1.0.3.jar
hbase-0.94.1-secrity.jar
log4j-1.2.16.jar
protobuf-java-2.4.0a.jar
slf4j-api-1.4.3.jar
slf4j-log4j12-1.4.3.jar
zookeeper-3.4.3.jar


=========Descrription of Experiment=======
1 Generate the fake Bixi data with both Uniform Distribution and Zipf Distribution

2 Create the table schema with the given table schema format

3 Upload the data into HBase with the given table schema

4 Execute the queries to get the query metrics under that table schema






















