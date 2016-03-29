#!/usr/bin/env sh

./setupFolders.sh

# Add oozie workflow files

hadoop fs -put ../${deployOozieDir}/* ${oozieAppHdfsPath}
echo "Uploaded oozie workflow files"

# Add dependency jar files

hadoop fs -put ../${deployLibsDir}/* ${oozieAppLibsHdfsPath}
echo "Uploaded oozie lib files"

# Add main app jar file
hadoop fs -put ../*.jar ${oozieAppLibsHdfsPath}
echo "Uploaded app code files"


if ! hadoop fs -test -e ${etlHdfsMahoutOutput}; then
	
	hadoop fs -mkdir ${etlHdfsMahoutOutput}
	echo "Created empty mahout output data file"
fi

if ! hadoop fs -test -e ${analysisHdfsPath}; then
	
	hadoop fs -mkdir ${analysisHdfsPath}
	echo "Created empty analysis dir"
	
fi

hadoop fs -put ../${deployEtlInputDataDir}/ReactorResearch.xls ${etlHdfsImportPath}
echo "Uploaded etl input data"