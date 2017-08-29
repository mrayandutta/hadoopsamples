package ayan.loginsight.logprocessorwithmip;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
  
public class LogFileDriverWithMIP extends Configured implements Tool
{
	private final static Logger logger = Logger.getLogger(LogFileDriverWithMIP.class);
    public static void main(String[] args) throws Exception 
    {
        int response =ToolRunner.run(new Configuration(), new LogFileDriverWithMIP(), args);
        logger.info("response :"+response);
    }
  
    public int run(String[] args) throws Exception 
    {
          Configuration conf = getConf();
          conf.set("mapred.textoutputformat.separator", ",");
          conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864"); //64 MB
           
          Job job = new Job(conf, getClass().getName());
          job.setJarByClass(LogFileDriverWithMIP.class);
          job.setReducerClass(LogReducer.class);
          int noOfReducers =1;
          job.setNumReduceTasks(noOfReducers);
           
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(Text.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
           
          job.setOutputFormatClass(TextOutputFormat.class);
           
          String inputPathForLog =args[0];
          String inputputPathForDC =args[1];
          String outputPath =args[2];
          
          logger.info("inputPathForLog:"+inputPathForLog);
          logger.info("inputputPathForDC:"+inputputPathForDC);
          
          MultipleInputs.addInputPath(job, new Path(inputPathForLog), TextInputFormat.class, LogMapper.class);
          MultipleInputs.addInputPath(job, new Path(inputputPathForDC), TextInputFormat.class, DCMapper.class);
          FileOutputFormat.setOutputPath(job, new Path(outputPath));
         
          int status =job.waitForCompletion(true)?0:1;
          logger.info("status:"+status);
          return status;
    }
  
}