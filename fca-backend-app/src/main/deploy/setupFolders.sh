#!/usr/bin/env sh

# Create app hdfs folder

if hadoop fs -test -d ${baseAppHdfsPath}; then
	hadoop fs -rm -r ${baseAppHdfsPath}
fi

hadoop fs -mkdir -p ${baseAppHdfsPath}

# Create oozie app folder

if hadoop fs -test -d ${oozieAppHdfsPath}; then
	hadoop fs -rm -r ${oozieAppHdfsPath}
fi

hadoop fs -mkdir -p ${oozieAppHdfsPath}

# Create oozie libs folder

if hadoop fs -test -d ${oozieAppLibsHdfsPath}; then
	hadoop fs -rm -r ${oozieAppLibsHdfsPath}
fi

hadoop fs -mkdir -p ${oozieAppLibsHdfsPath}

# Create etl hdfs folders
if ! hadoop fs -test -d ${etlHdfsPath}; then
	hadoop fs -mkdir -p ${etlHdfsPath}
fi

if ! hadoop fs -test -d ${etlHdfsImportPath}; then
	hadoop fs -mkdir -p ${etlHdfsImportPath}
fi