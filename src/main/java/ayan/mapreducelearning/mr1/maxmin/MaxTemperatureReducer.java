package ayan.mapreducelearning.mr1.maxmin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                        throws IOException, InterruptedException {
                
                IntWritable maxTemperature = new IntWritable(Integer.MIN_VALUE);
                for (IntWritable value : values) {
                        if (value.get() > maxTemperature.get())
                                maxTemperature = value;
                }
                
                context.write(key, maxTemperature);
        }
}