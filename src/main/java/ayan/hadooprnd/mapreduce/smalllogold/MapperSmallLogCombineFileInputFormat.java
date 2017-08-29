package ayan.hadooprnd.mapreduce.smalllogold;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
 
public class MapperSmallLogCombineFileInputFormat extends MapReduceBase implements
		Mapper<LongWritable, Text, SmallLogKey, Text> {
 
	Text txtValue = new Text("");
 
	public void map(LongWritable key, Text value,
			OutputCollector<SmallLogKey, Text> output, Reporter reporter)
			throws IOException {
		
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
            txtValue.set(valueStr);
            
            SmallLogKey smallLogKey = new SmallLogKey(keyStr,valueStr);
            output.collect(smallLogKey, txtValue);
        }
 
	}
 
}