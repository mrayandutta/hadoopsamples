package ayan.hadooprnd.mapreduce.smalllog;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SmallLogCombineRecordReader extends RecordReader<LongWritable,Text> {
 
    private Path path;
 
    private LineRecordReader lineReader = null;
    private int index;
 
    public SmallLogCombineRecordReader(CombineFileSplit split,TaskAttemptContext context,Integer index) throws IOException {
        this.path = split.getPath(index);
        this.index = index;
    }
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
           CombineFileSplit fileSplit = (CombineFileSplit)inputSplit;
            lineReader= new LineRecordReader();
           FileSplit fileSplit1  = new FileSplit(fileSplit.getPath(index),fileSplit.getOffset(this.index),fileSplit.getLength(),fileSplit.getLocations());
           lineReader.initialize(fileSplit1,taskAttemptContext);
    }
 
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
 
        return lineReader.nextKeyValue();
 
    }
 
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return lineReader.getCurrentKey();
    }
 
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return lineReader.getCurrentValue();
    }
 
    @Override
    public float getProgress() throws IOException, InterruptedException {
/*
        if (startOffset == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - startOffset) / (float)(end - startOffset));
        }
  */
 
        return 0.0f;
    }
 
    @Override
    public void close() throws IOException {
            if(lineReader!=null){
                lineReader.close();;
                lineReader = null;
            }
    }
}