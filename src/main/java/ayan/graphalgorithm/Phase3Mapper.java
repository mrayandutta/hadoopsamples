package ayan.graphalgorithm;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Phase3Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String valueStr = value.toString();
		String[] keyValueArray = valueStr.split("\\s+");
		String customer = keyValueArray[0];
		String ipArrayStr = keyValueArray[1];
		Map<String,List<String>> ipPairMap = MRUtil.getIPPairMap(Arrays.asList(ipArrayStr.split(",")));
		for (String ip:ipPairMap.keySet()) {
			List<String> pairs = ipPairMap.get(ip);
			for (String pair: pairs) {
				context.write(new Text(ip), new Text(pair+","+customer));
			}
			
		}
	}
}
