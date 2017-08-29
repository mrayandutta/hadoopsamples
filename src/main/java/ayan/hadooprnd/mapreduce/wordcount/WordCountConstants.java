package ayan.hadooprnd.mapreduce.wordcount;

import java.io.File;

public interface WordCountConstants 
{
	String HDFS_URL="hdfs://10.0.2.15:8020";
	String HDFS_INPUT_DIR="data";
	String HDFS_INPUT_FILE="weblog.txt";
	
	String HDFS_OUTPUT_DIR="mroutput";
	
	String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+HDFS_INPUT_DIR+File.separator+HDFS_INPUT_FILE;
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR;

	
	

}
