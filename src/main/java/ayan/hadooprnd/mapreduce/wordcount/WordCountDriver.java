package ayan.hadooprnd.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver 
{
	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "wordcount");
		 
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		//job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String inputPath =WordCountConstants.HDFS_INPUT_LOCATION;
		String outputPath =WordCountConstants.HDFS_OUTPUT_LOCATION;
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.submit();
		System.out.println(job.waitForCompletion(true));
	}

}
