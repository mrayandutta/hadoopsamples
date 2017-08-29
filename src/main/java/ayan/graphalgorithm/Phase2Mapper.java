package ayan.graphalgorithm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Phase2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] keyValueArray = valueStr.split("\\s+");
		String ipPair = keyValueArray[0];
		String customerStr = keyValueArray[1];
		context.write(new Text(ipPair),new Text(customerStr));
	}
}
