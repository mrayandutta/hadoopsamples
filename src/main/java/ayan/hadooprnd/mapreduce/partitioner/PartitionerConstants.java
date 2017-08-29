package ayan.hadooprnd.mapreduce.partitioner;

import java.io.File;

public interface PartitionerConstants 
{
	String HDFS_URL="hdfs://10.0.2.15:8020";
	
	String PARTITIONER_FILE_NAME="partitioner.txt";
	String HDFS_INPUT_DIR="data"+File.separator;
	
	String HDFS_OUTPUT_DIR="logoutputdata";
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR+File.separator;
	
	String INPUT_PATH=HDFS_URL+File.separator+HDFS_INPUT_DIR+PARTITIONER_FILE_NAME;
	String OUTPUT_PATH=HDFS_OUTPUT_LOCATION;
}
