package ayan.hadooprnd.mapreduce.useraccess;

import java.io.File;

public interface MultipleFileConstants 
{
	String HDFS_URL="hdfs://10.0.2.15:8020";
	String HDFS_INPUT_DIR="logdata/app1";
	
	String HDFS_OUTPUT_DIR="logoutputdata";
	
	String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+HDFS_INPUT_DIR;
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR+File.separator;

	
	

}
