package ayan.loginsight.logprocessor.distributedcache;
  
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import ayan.loginsight.custominputformat.CustomCombinedInputFormat;
  
public class LogFileDriverWithDistributedCache extends Configured implements Tool
{
	private final static Logger logger = Logger.getLogger(LogFileDriverWithDistributedCache.class);
    public static void main(String[] args) throws Exception 
    {
        int response =ToolRunner.run(new Configuration(), new LogFileDriverWithDistributedCache(), args);
        logger.info("response :"+response);
    }
  
    public int run(String[] args) throws Exception 
    {
          Configuration conf = getConf();
          conf.set("mapred.textoutputformat.separator", ",");
          conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864"); //64 MB
           
          Job job = new Job(conf, getClass().getName());
          String inputPath =args[0];
          String outputPath =args[1];
          String distributedCacheURI =args[2];
          DistributedCache.addCacheFile(new URI(distributedCacheURI), job.getConfiguration());
           
          job.setJarByClass(LogFileDriverWithDistributedCache.class);
          job.setMapperClass(LogFileMapperWithDistributedCache.class);
          int noOfReducers =1;
          job.setNumReduceTasks(noOfReducers);
           
          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(Text.class);
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(Text.class);
           
          job.setInputFormatClass(CustomCombinedInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);
           
          FileInputFormat.addInputPath(job, new Path(inputPath));
          FileOutputFormat.setOutputPath(job, new Path(outputPath));
         
          int status =job.waitForCompletion(true)?0:1;
          logger.info("status:"+status);
          return status;
    }
  
}