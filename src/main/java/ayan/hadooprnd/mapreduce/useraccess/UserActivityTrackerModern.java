package ayan.hadooprnd.mapreduce.useraccess;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class UserActivityTrackerModern 
{

	enum MYCOUNTER 
	{
		RECORD_COUNT, FILE_EXISTS, LOAD_MAP_ERROR
	}
  public static void main(String[] args) throws Exception 
  {
	  Configuration conf = new Configuration();
	  //DistributedCache.addCacheFile(new URI(UserAccessConstants.HDFS_KPI_LOCATION), conf);
	  Job job = new Job(conf, "UserActivityTrackerModern");
	  //DistributedCache.addCacheFile(new URI(UserAccessConstants.HDFS_INPUT_LOCATION), conf);
	  //DistributedCache.addCacheArchive(new URI("/user/akhanolk/joinProject/data/departments_map.tar.gz"),conf);

	  
	  job.setJarByClass(UserActivityTrackerModern.class);
	  job.setMapperClass(UserLogMapper.class);
	  
	  job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
    String inputPath =UserAccessConstants.HDFS_INPUT_LOCATION;
    String outputPath =UserAccessConstants.HDFS_OUTPUT_LOCATION;
    System.out.println("inputPath:"+inputPath);
    System.out.println("outputPath:"+outputPath);
    
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    boolean result =job.waitForCompletion(true);
    long totalRecords =job.getCounters().findCounter(MYCOUNTER.RECORD_COUNT).getValue();
    
    System.out.println("result:"+result+",totalRecords:"+totalRecords);
    
    }
}
