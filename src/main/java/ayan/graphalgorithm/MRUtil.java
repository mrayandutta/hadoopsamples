package ayan.graphalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

public class MRUtil {
	
	public static List<String> getIPPairList(List<String> ipList)
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
	
	public static Map<String,List<String>> getIPPairMap(List<String> ipList)
	{
		Map<String,List<String>> ipPairMap = new HashMap<String,List<String>>();
		List<String> ipPairList = getIPPairList(ipList);
		
		for (String ip : ipList) 
		{
			List<String> filteredPairList = new ArrayList<String>(); 
			for(String pair :ipPairList)
			{
				String ipArray[] = pair.split(",");
				if(ip.equals(ipArray[0]) || ip.equals(ipArray[1]))
				{
					filteredPairList.add(pair);
				}
			}
			ipPairMap.put(ip, filteredPairList);
		}
		System.out.println("ipPairMap:"+ipPairMap);
		return ipPairMap;
	}
	
	public static String getCustomerString(Iterable<Text> customers)
	{
		StringBuffer sb = new StringBuffer();
		for (Text customer: customers) 
		{
			String customersStr = customer.toString();
			sb.append(customersStr);
			sb.append(",");
		}
		//Remove extra comma at the end 
		if(sb.length()>0)
		{
			sb.deleteCharAt(sb.length()-1);
		}
		System.out.println("sb:"+sb);
		return sb.toString();
	}
	
	public static String getDerivedIPString(Iterable<String> ips)
	{
		StringBuffer sb = new StringBuffer();
		for (String ip: ips) 
		{
			sb.append(ip);
			sb.append(",");
		}
		//Remove extra comma at the end 
		if(sb.length()>0)
		{
			sb.deleteCharAt(sb.length()-1);
		}
		System.out.println("sb:"+sb);
		return sb.toString();
	}
	
	public static void main(String[] args) {
		List<String>  ipList = Arrays.asList(new String[] {"IP1", "IP2","IP3"});
		getIPPairMap(ipList);
	}

}
