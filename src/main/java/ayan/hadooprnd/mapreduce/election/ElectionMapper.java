package ayan.hadooprnd.mapreduce.election;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	Text candidateKey = new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
	{
		String line = value.toString();
        String[] inputArray = line.split("\\s+");
        String candidateName = inputArray[0];
        candidateKey.set(candidateName);
        String voteCount = inputArray[1];
        //System.out.println("candidate:"+candidateName);
        
        int votes = Integer.parseInt(voteCount);
        context.write(candidateKey, new IntWritable(votes));
	}
}
