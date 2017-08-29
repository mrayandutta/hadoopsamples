package ayan.hadooprnd.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartitionerMapReduceDriver
{
 
    public static void main(String[] args) throws Exception 
    {
      Configuration conf = new Configuration();
      // Create job
      Job job = new Job(conf, "Tool Job");
      job.setJarByClass(PartitionerMapReduceDriver.class);

      // Setup MapReduce job
      // Do not specify the number of Reducer
      job.setMapperClass(PartitionMapper.class);
      job.setReducerClass(ParitionReducer.class);
      int noOfReduceTasks =3;
      job.setNumReduceTasks(noOfReduceTasks);

      // Specify key / value
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Input
      job.setInputFormatClass(TextInputFormat.class);

      // Output
      job.setOutputFormatClass(TextOutputFormat.class);
      
      String inputPath =PartitionerConstants.INPUT_PATH;
      String outputPath =PartitionerConstants.OUTPUT_PATH;
      
      job.setPartitionerClass(AgePartitioner.class);
      
      System.out.println("inputpath :"+inputPath);
      System.out.println("outputPath :"+outputPath);
      
      FileInputFormat.setInputPaths(job, new Path(PartitionerConstants.INPUT_PATH));
      FileOutputFormat.setOutputPath(job, new Path(PartitionerConstants.OUTPUT_PATH));

      // Execute job and return status
       
       boolean result =job.waitForCompletion(true);
       System.out.println("result:"+result);

    }
 
   
}