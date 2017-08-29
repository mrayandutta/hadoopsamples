package ayan.hadooprnd.mapreduce.smalllog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SmallLogDriver 
{
	public static void main(String[] args) throws Exception 
	  {
		  Configuration conf = new Configuration();
		  Job job = new Job(conf, "SmallLogDriver");
		  
		  job.setJarByClass(SmallLogDriver.class);
		  job.setMapperClass(SmallLogMapper.class);
		  
		  job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      String inputPath =args[0];
	      String outputPath =args[1];
	      System.out.println("inputPath:"+inputPath);
	      System.out.println("outputPath:"+outputPath);
	    
	      FileInputFormat.addInputPath(job, new Path(inputPath));
	      //FileInputFormat.setInputPaths(job, new Path(inputPath));
	      FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	      boolean result =job.waitForCompletion(true);
	    
	      System.out.println("result:"+result);
	    
	    }

}
