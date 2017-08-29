package ayan.loginsight.logprocessorwithmip;
  
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
  
public class DCMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    private Text userIdKey = new Text();
    private Text userNameText = new Text();
     
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        //Record format is userid,username
        String[] inputArray = line.split(",");
        userIdKey.set(inputArray[0]);
        userNameText.set(inputArray[1]);
        context.write(userIdKey, userNameText);
    }
     
  }