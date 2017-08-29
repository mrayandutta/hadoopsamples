package ayan.hadooprnd.mapreduce.useraccess;

import java.io.File;

public interface UserAccessConstants 
{
	String HDFS_URL="hdfs://10.0.2.15:8020";
	String HDFS_INPUT_DIR="data"+File.separator;;
	
	String HDFS_OUTPUT_DIR="logoutputdata";
	
	String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+HDFS_INPUT_DIR;
	String HDFS_KPI_LOCATION=HDFS_URL+File.separator+"kpidata"+File.separator+"kpi.csv";
	String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+HDFS_OUTPUT_DIR+File.separator;

	
	

}
