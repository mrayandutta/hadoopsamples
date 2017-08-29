package ayan.graphalgorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChainedJobs extends Configured implements Tool 
{
	private static final String OUTPUT_PATH = "/CHAINEDJOBS_TEMP";

	 public int run(String[] args) throws Exception 
	 {
	  //Job 1
	  Configuration conf = getConf();
	  FileSystem fs = FileSystem.get(conf);
	  Job job = new Job(conf, "Phase1Job");
	  job.setJarByClass(this.getClass());

	  job.setMapperClass(Phase1Mapper.class);
	  job.setReducerClass(Phase1Reducer.class);

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);

	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);

	  TextInputFormat.addInputPath(job, new Path(args[0]));
	  TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

	  job.waitForCompletion(true);

	   // Job 2
	  Configuration conf2 = getConf();
	  Job job2 = new Job(conf2, "Phase2Job");
	  job2.setJarByClass(this.getClass());

	  job2.setMapperClass(Phase2Mapper.class);
	  job2.setReducerClass(Phase2Reducer.class);

	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);

	  job2.setInputFormatClass(TextInputFormat.class);
	  job2.setOutputFormatClass(TextOutputFormat.class);

	  TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
	  TextOutputFormat.setOutputPath(job2, new Path(args[1]));

	  return job2.waitForCompletion(true) ? 0 : 1;
	 }

	 public static void main(String[] args) throws Exception {
	  ToolRunner.run(new Configuration(), new ChainedJobs(), args);
	 }

}
