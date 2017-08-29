package ayan.hadooprnd.mapreduce.loganalysis;

import java.io.File;

public interface LogAnalysisConstants 
{
	String HDFS_URL="hdfs://192.168.64.128:8020";
	String HDFS_INPUT_DIR="data";
	String HDFS_INPUT_FILE="webLog.txt";
	
	String HDFS_OUTPUT_DIR="abcd";
	
	String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+HDFS_INPUT_DIR+File.separator+HDFS_INPUT_FILE;
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR;

	
	

}
