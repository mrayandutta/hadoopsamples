package ayan.hadooprnd.mapreduce.useraccess;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class UserActivityTracker 
{
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
  {
    private Text userKey = new Text();
    private Text valueText = new Text();
    public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
    	
    	String line = value.toString();
        String[] inputArray = line.split("\\s+");
        if(inputArray!=null && inputArray.length==4)
        {
        	String timestamp = inputArray[0];
            String user = inputArray[1];
            String action = inputArray[2];
            String ip = inputArray[3];
            
            String keyStr = user.trim();
            String valueStr = action+","+timestamp+","+ip;
            
            //System.out.println("key:"+keyStr);
            //System.out.println("value:"+valueStr);
            userKey.set(keyStr);
            valueText.set(valueStr);
            output.collect(userKey, valueText);
        }
        else
        {
        	System.out.println("Invalid Record !!!,inputArray:"+inputArray);
        }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
    {
        output.collect(key, key);
    }
  }

  public static void main(String[] args) throws Exception 
  {
    JobConf conf = new JobConf(UserActivityTracker.class);

    conf.setJobName("User Activity Tracker");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    //conf.setReducerClass(Reduce.class);

    conf.set("mapred.textoutputformat.separator", ",");
    conf.set("mapred.map.tasks", "5");
    conf.setInputFormat(TextInputFormat.class);
    //conf.setInputFormat(CombinedInputFormat.class);
    //conf.set("mapred.max.split.size", "134217728"); // 128 MB
    conf.setOutputFormat(TextOutputFormat.class);
    
    //String inputPath =UserAccessConstants.HDFS_INPUT_LOCATION;
    //String outputPath =UserAccessConstants.HDFS_OUTPUT_LOCATION;
    //System.out.println("inputPath:"+inputPath);
    //System.out.println("outputPath:"+outputPath);
    
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient jobClient = new JobClient();
    jobClient.runJob(conf);
    
    System.out.println("No of Tasks:"+(conf.get("mapred.map.tasks")));
    
    }
}
