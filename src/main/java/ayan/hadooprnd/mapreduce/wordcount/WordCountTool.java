package ayan.hadooprnd.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountTool extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
	{
		int response =ToolRunner.run(new Configuration(), new WordCountTool(),args);
	}
	
	public int run(String[] args) throws Exception 
	{
		Configuration conf = getConf();
		System.out.println("Prop1 :"+conf.get("prop1"));
		System.out.println("Prop2 :"+conf.get("prop2"));
		
		Job job = new Job(conf, "WordCountTool");
		 
		job.setJarByClass(WordCountTool.class);
		job.setMapperClass(WordCountMapper.class);
		//job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String inputPath =args[0];
		String outputPath =args[1];
		
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.submit();
		int status = job.waitForCompletion(true)?0:1;
		System.out.println("Final Status:"+status);
		return status;
	}

}
