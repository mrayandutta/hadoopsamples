package ayan.hadooprnd.mapreduce.election;

import java.io.File;

public interface ElectionConstants 
{
	String HDFS_URL="hdfs://10.0.2.15:8020";
	String HDFS_INPUT_DIR="data"+File.separator;;
	
	String HDFS_OUTPUT_DIR="electionoutputdata";
	
	String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+HDFS_INPUT_DIR;
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR+File.separator;

	
	

}
