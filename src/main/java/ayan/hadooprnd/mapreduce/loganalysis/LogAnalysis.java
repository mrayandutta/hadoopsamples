package ayan.hadooprnd.mapreduce.loganalysis;

import java.io.IOException;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import ayan.hadooprnd.mapreduce.wordcount.WordCount;

public class LogAnalysis 
{
	public static class LogMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			String line = value.toString();
			String lineSplits[] =line.split("");
			StringBuffer mapperValue=new StringBuffer();
			for (int i = 0; i < lineSplits.length; i++) 
			{
				mapperValue.append(lineSplits[i]);
				if(i<(lineSplits.length-1))
				{
					mapperValue.append(",");
				}
			}
			Text valueStr = new Text(mapperValue.toString());
			Text blankText = new Text("");
			output.collect(valueStr, blankText);
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		JobConf jobConf = new JobConf(WordCount.class);
		jobConf.setJobName("wordcount");
		//conf.set("mapred.textoutputformat.separator", ";");
		jobConf.setMapperClass(LogMapper.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(jobConf, new Path(LogAnalysisConstants.HDFS_INPUT_LOCATION));
		FileOutputFormat.setOutputPath(jobConf, new Path(LogAnalysisConstants.HDFS_OUTPUT_LOCATION));
		
		JobClient jobclient = new JobClient(jobConf);
		RunningJob runjob = jobclient.submitJob(jobConf);          
	}
}
