package ayan.hadooprnd.mapreduce.smalllog;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class SmallLogCombineInputFormat extends CombineFileInputFormat<LongWritable, Text> 
{
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException 
    {
       CombineFileSplit combineSplit = (CombineFileSplit) inputSplit;
       System.out.println(" length is  "+combineSplit.getLength());
       return new CombineFileRecordReader<LongWritable, Text>(combineSplit,taskAttemptContext,SmallLogCombineRecordReader.class) ;
    }
 
}
