package ayan.graphalgorithm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Phase1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] keyValueArray = valueStr.split("\\s+");
		String ip = keyValueArray[0];
		String customerStr = keyValueArray[1];
		
		
		String[] customerArray = customerStr.split(",");
		for (int i = 0; i < customerArray.length; i++) {
			context.write(new Text(customerArray[i]),new Text(ip));
		}
	}
}
