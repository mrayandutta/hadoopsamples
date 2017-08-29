package ayan.graphalgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase1Reducer extends Reducer<Text, Text, Text, Text> 
{
	@Override
	protected void reduce(Text customer, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException 
	{
		List<String> ipPairList = getIPPairList(getIPList(values));
		
		Iterator<String> itr = ipPairList.iterator();
		while (itr.hasNext()) 
		{
			String ipKey = itr.next();
			context.write(new Text(ipKey),customer);
			System.out.println("Added to context ,ipKey:"+ipKey+",customer value:"+customer);
		}
	}
	
	private List<String> getIPList(Iterable<Text> values)
	{
		List<String> ipList = new ArrayList<String>();
		for (Text ip: values) 
		{
			String ipStr = ip.toString();
			ipList.add(ipStr);
		}
		
		System.out.println("ipList:"+ipList);
		return ipList;
	}
	
	private List<String> getIPPairList(List<String> ipList)
	{
		List<String> ipPairList = new ArrayList<String>();
		for (int i = 0; i < ipList.size(); i++) 
		{
			System.out.println("i:"+i);
			for (int j = i+1; j < ipList.size(); j++) 
			{
				String ip = ipList.get(i);
				String nextIp = ipList.get(j);
				ipPairList.add(ip+","+nextIp);
			}
		}
		System.out.println("ipPairList:"+ipPairList);
		return ipPairList;
	}
}
