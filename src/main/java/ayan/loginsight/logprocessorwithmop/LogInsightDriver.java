package ayan.loginsight.logprocessorwithmop;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ayan.loginsight.custominputformat.CustomCombinedInputFormat;
  
public class LogInsightDriver extends Configured implements Tool
{
    public static void main(String[] args) throws Exception 
    {
        int response =ToolRunner.run(new Configuration(), new LogInsightDriver(), args);
        System.out.println("response :"+response);
    }
  
    public int run(String[] args) throws Exception 
    {
          Configuration conf = getConf();
          conf.set("mapred.textoutputformat.separator", ",");
          //conf.set("mapreduce.input.fileinputformat.split.maxsize", "125829120"); // 120 MB
          conf.set("mapreduce.input.fileinputformat.split.maxsize", "64000000"); // 64 MB
          
           
          Job job = new Job(conf, getClass().getSimpleName());
           
          job.setJarByClass(LogInsightDriver.class);
          job.setMapperClass(LogInsightMapper.class);
          int noOfReducers =0;
          job.setNumReduceTasks(noOfReducers);
           
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(Text.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
           
          job.setInputFormatClass(CustomCombinedInputFormat.class);
          //job.setInputFormatClass(TextInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);
          /*
          A typical map reduce program can produce output files that are empty, depending on your implementation.
          If you want to suppress creation of empty files, you need to leverage LazyOutputFormat.
          */ 
          LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
           
          String inputPath =args[0];
          String outputPath =args[1];
         
          FileInputFormat.addInputPath(job, new Path(inputPath));
          FileOutputFormat.setOutputPath(job, new Path(outputPath));
         
          int status =job.waitForCompletion(true)?0:1;
          System.out.println("status:"+status);
          return status;
    }
  
}