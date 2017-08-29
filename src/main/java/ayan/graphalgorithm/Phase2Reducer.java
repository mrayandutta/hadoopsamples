package ayan.graphalgorithm;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase2Reducer extends Reducer<Text, Text, Text, Text> 
{
	@Override
	protected void reduce(Text ipPair, Iterable<Text> customers,
			Context context)
			throws IOException, InterruptedException 
	{
		String customerStr = MRUtil.getCustomerString(customers);
		context.write(new Text(ipPair),new Text(customerStr));
		System.out.println("Added to context ,ipPair:"+ipPair+",customerStr:"+customerStr);
	}
	
	
	
}
