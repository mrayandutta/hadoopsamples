package ayan.mapreducelearning.mr1.wordcount;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CharFilterDriver extends Configured implements Tool 
{
	
	private static List<String> invalidAsciiList = new ArrayList<String>();
	static
	{
		for (int i=0;i<=26;i++)
		{
			String  asciiCharToBeFiltered= Character.toString((char)i);
			invalidAsciiList.add(asciiCharToBeFiltered);
		}
		
		//System.out.println("ascii list :"+invalidAsciiList);
		invalidAsciiList.remove(10);
	}
	
	public static String removeInvalidCharsFromString(String input)
	{
		String output = input;
		for (String invalidAsciiChar: invalidAsciiList) 
		{
			output = output.replaceAll(invalidAsciiChar, " ");
		}
		return output;
	}
	
	public static void main(String[] args) throws Exception 
    {
        int response =ToolRunner.run(new Configuration(), new CharFilterDriver(), args);
        System.out.println("response :"+response);
    }
	
	public int run(String[] args) throws Exception 
    {
          Configuration conf = getConf();
          conf.set("textinputformat.record.delimiter","------------");
           
          Job job = new Job(conf, getClass().getSimpleName());
           
          job.setJarByClass(getClass());
          job.setMapperClass(FilteringingMapper.class);
          job.setNumReduceTasks(0);
           
           
          job.setInputFormatClass(TextInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);
           
          String inputPath =args[0];
          String outputPath =args[1];
         
          FileInputFormat.addInputPath(job, new Path(inputPath));
          FileOutputFormat.setOutputPath(job, new Path(outputPath));
         
          int status =job.waitForCompletion(true)?0:1;
          System.out.println("status:"+status);
          return status;
    }
	
	public static class FilteringingMapper extends Mapper<LongWritable, Text, Text, Text>
    {
    	public void map(LongWritable key, Text lineText ,Context context) throws IOException, InterruptedException 
        {
    		String line = lineText.toString();
    		//String filteredLine = removeInvalidCharsFromString(line);
    		
    		
    		//context.write(new Text("old line:"), new Text(lineText));
    		//context.write(new Text("new line:"), new Text(filteredLine));
    		context.write(new Text(lineText), new Text(""));
        }
    }
	
		
}
