package ayan.mapreducelearning.mr1.maxmin;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class YearTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map(LongWritable offset, Text value, Context context) 
            throws IOException, InterruptedException {
        
        StringTokenizer tok = new StringTokenizer(value.toString());
        int year = Integer.decode(tok.nextToken(","));
        
        // eat next field -- stationId (unused)
        Integer.decode(tok.nextToken(","));
        
        int recordedTemperature = Integer.decode(tok.nextToken(","));
        
        IntWritable newKey = new IntWritable(year);
        IntWritable newValue = new IntWritable(recordedTemperature);
        context.write(newKey, newValue);
        }
}