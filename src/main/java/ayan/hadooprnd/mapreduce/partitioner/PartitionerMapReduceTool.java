package ayan.hadooprnd.mapreduce.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionerMapReduceTool extends Configured implements Tool 
{
 
    public static void main(String[] args) throws Exception 
    {
        int res = ToolRunner.run(new Configuration(), new PartitionerMapReduceTool(), args);
        System.exit(res);
    }
 
    public int run(String[] args) throws Exception 
    {
        // When implementing tool
        Configuration conf = this.getConf();
 
        // Create job
        Job job = new Job(conf, "Tool Job");
        job.setJarByClass(PartitionerMapReduceTool.class);
 
        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(ParitionReducer.class);
 
        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, new Path(PartitionerConstants.INPUT_PATH));
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, new Path(PartitionerConstants.OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

	public Configuration getConf() 
	{
		return null;
	}

	public void setConf(Configuration arg0) 
	{
	}
}