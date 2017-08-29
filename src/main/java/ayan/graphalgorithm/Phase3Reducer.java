package ayan.graphalgorithm;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase3Reducer extends Reducer<Text, Text, Text, Text> 
{
	@Override
	protected void reduce(Text ipPair, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException 
	{
		Set allIPSet = new HashSet();
        Map<String,Set<String>> customerToIPListMap = new HashMap<String,Set<String>>();
        Set allCustomerSet = new HashSet();
        
        for (Text value: values)
        { 
	         String valueStr = value.toString();
	         String[] valueStrArray = valueStr.split(",");
			 String firstIP = valueStrArray[0];
	         String secondIP = valueStrArray[1];
	         String customer = valueStrArray[2];
	        
	         allIPSet.add(firstIP);
	         allIPSet.add(secondIP);
	         allCustomerSet.add(customer);
	        
	         Set<String> ipSet = new HashSet<String>();
	         if(customerToIPListMap.containsKey(customer))
	         {
	           ipSet = customerToIPListMap.get(customer);
	         }
	        
	         ipSet.add(firstIP);
	         ipSet.add(secondIP);
	         customerToIPListMap.put(customer, ipSet);
        } 
        
        for (String customer :customerToIPListMap.keySet())
        {
           Set<String> availableIPSet = customerToIPListMap.get(customer);
           Collection derivedIPSet =CollectionUtils.subtract(allIPSet, availableIPSet);
           if(!derivedIPSet.isEmpty())
           {
                 System.out.println("customer:"+customer+",derivedIPSet:"+derivedIPSet);
                 context.write(new Text(customer), new Text(MRUtil.getDerivedIPString(derivedIPSet)));
           }
        }
	}
}
