package ayan.hadooprnd.mapreduce.custominputformat.edureka;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogRecordMapper extends Mapper<LogRecordKey, LogRecordValue, Text, Text> {
        
          protected void map(LogRecordKey key, LogRecordValue value, Context context)
              throws java.io.IOException, InterruptedException {
        	  
            String sensor = key.getsensorType().toString();
            
            if(sensor.toLowerCase().equals("a")){
            	context.write(value.getValue1(),value.getValue2());
            }
            		
          }  
}