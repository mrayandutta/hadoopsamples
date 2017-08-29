package ayan.hadooprnd.mapreduce.wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount 
{

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
	{

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException 
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) 
			{
				value.set(tokenizer.nextToken());
				output.collect(value, new IntWritable(1));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException 
		{
			int sum = 0;
			while (values.hasNext()) 
			{
				sum += values.next().get();
			}
			System.out.println("key:"+key+",sum:"+sum);
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception 
	{

		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		//conf.set("mapred.textoutputformat.separator", ";");
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(WordCountConstants.HDFS_INPUT_LOCATION));
		FileOutputFormat.setOutputPath(conf, new Path(WordCountConstants.HDFS_OUTPUT_LOCATION));
		
		JobClient jobclient = new JobClient(conf);
		RunningJob runjob = jobclient.submitJob(conf);          
   	    TaskReport [] maps = jobclient.getMapTaskReports(runjob.getID());

		 long mapDuration = 0;
		 for(TaskReport rpt: maps)
		 {
		    mapDuration += rpt.getFinishTime() - rpt.getStartTime();
		    
		    System.out.println("maps length:"+maps.length);
		 }
	}
}
