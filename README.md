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
