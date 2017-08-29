package ayan.hadooprnd.mapreduce.election;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ElectionDriverWithCombiner 
{
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException 
	{

		  Configuration conf = new Configuration();
		  Job job = new Job(conf, "ElectionDriverWithCombiner");
		  
		  job.setJarByClass(ElectionDriverWithCombiner.class);
		  job.setMapperClass(ElectionMapper.class);
		  job.setReducerClass(ElectionReducer.class);
		  job.setCombinerClass(ElectionReducer.class);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);
	      
	      job.setNumReduceTasks(5);
	      
	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      String inputPath =args[0];
	      String outputPath =args[1];
	      
	    //String inputPath =ElectionConstants.HDFS_INPUT_LOCATION;
	    //String outputPath =ElectionConstants.HDFS_OUTPUT_LOCATION;
	    System.out.println("inputPath:"+inputPath);
	    System.out.println("outputPath:"+outputPath);
	    
	    FileInputFormat.setInputPaths(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    boolean result =job.waitForCompletion(true);
	    
	    System.out.println("result:"+result);
	    
	    
	}

}
