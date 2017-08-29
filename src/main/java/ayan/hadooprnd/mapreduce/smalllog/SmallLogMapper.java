package ayan.hadooprnd.mapreduce.smalllog;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SmallLogMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    private Text userKey = new Text();
    private Text valueText = new Text();
    
    
    
    public void map(Text key, Text value,Context context) throws IOException, InterruptedException 
    {
    	String line = value.toString();
        String[] inputArray = line.split("\\s+");
        if(inputArray!=null && inputArray.length==4)
        {
        	String timestamp = inputArray[0];
            String user = inputArray[1];
            String action = inputArray[2];
            String ip = inputArray[3];
            
            String keyStr = user.trim();
            String valueStr = action+","+timestamp+","+ip;
            
            //System.out.println("key:"+keyStr);
            //System.out.println("value:"+valueStr);
            userKey.set(keyStr);
            valueText.set(valueStr);
            context.write(userKey, valueText);
        }
        else
        {
        	System.out.println("Invalid Record !!!,inputArray:"+inputArray);
        }
    }
  }
