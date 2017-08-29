package ayan.hadooprnd.mapreduce.custominputformat;

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



public class ActivityTrackerWithoutCIFormat 
{

	enum MYCOUNTER 
	{
		RECORD_COUNT, FILE_EXISTS, LOAD_MAP_ERROR
	}
  public static void main(String[] args) throws Exception 
  {
	  Configuration conf = new Configuration();
	  Job job = new Job(conf, "ActivityTrackerWithoutCIFormat");
	  
	  job.setJarByClass(ActivityTrackerWithoutCIFormat.class);
	  job.setMapperClass(LogMapper.class);
	  
	  job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setNumReduceTasks(5);
      
    String inputPath =args[0];
    String outputPath =args[1];
    System.out.println("inputPath:"+inputPath);
    System.out.println("outputPath:"+outputPath);
    
    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    boolean result =job.waitForCompletion(true);
    long totalRecords =job.getCounters().findCounter(MYCOUNTER.RECORD_COUNT).getValue();
    
    System.out.println("result:"+result+",totalRecords:"+totalRecords);
    
    }
}
