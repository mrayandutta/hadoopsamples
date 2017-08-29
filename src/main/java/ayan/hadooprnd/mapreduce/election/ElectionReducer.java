package ayan.hadooprnd.mapreduce.election;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ElectionReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable,
			Context context)
			throws IOException, InterruptedException 
	{
		int voteCount = 0;
		Iterator<IntWritable> itr = iterable.iterator();
		while (itr.hasNext()) 
		{
			IntWritable intWritable = itr.next();
			voteCount =voteCount+ intWritable.get();
		}
		
		context.write(key, new IntWritable(voteCount));
	}

}
