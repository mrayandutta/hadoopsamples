package ayan.hadooprnd.mapreduce.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		  
		protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException 
		{
		String line = value.toString();
		System.out.println("line:"+line);
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) 
		{
		 word.set(tokenizer.nextToken());
		 context.write(word, one);
		}
	}
}