package ayan.hadooprnd.mapreduce.smalllog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallLogDriverCI extends Configured implements Tool
{
	public static void main(String[] args) throws Exception 
    {
		int res = ToolRunner.run(new Configuration(), new SmallLogDriverCI(), args);
        System.exit(res);
	}

	public int run(String[] args) throws Exception 
	{
		  Configuration conf = getConf();
		  //conf.set("mapred.max.split.size", "67108864");
		
		 //Configuration conf = new Configuration();
		  Job job = new Job(conf, "SmallLogDriverCI64");
		  
		  job.setJarByClass(SmallLogDriverCI.class);
		  job.setMapperClass(SmallLogMapper.class);
		  
		  job.setOutputKeyClass(LongWritable.class);
	      job.setOutputValueClass(Text.class);
	      
	      job.setInputFormatClass(SmallLogCombineInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);
	      
	      String inputPath =args[0];
	      String outputPath =args[1];
	      System.out.println("inputPath:"+inputPath);
	      System.out.println("outputPath:"+outputPath);
	    
	      FileInputFormat.setInputPaths(job, new Path(inputPath));
	      FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	      int result =job.waitForCompletion(true)?0:1;
	    
	      System.out.println("result:"+result);
	
	      return result;
	}

}
