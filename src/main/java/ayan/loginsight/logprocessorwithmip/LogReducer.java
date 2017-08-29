package ayan.loginsight.logprocessorwithmip;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text value = new Text();
	@Override
	protected void reduce(Text user, Iterable<Text> iterable,
			Context context)
			throws IOException, InterruptedException 
	{
		StringBuffer sb = new StringBuffer();
		Iterator<Text> itr = iterable.iterator();
		while (itr.hasNext()) 
		{
			Text record = itr.next();
			sb.append(record.toString()).append(",");
		}
		value.set(sb.toString());
		
		context.write(user, value);
	}

}
