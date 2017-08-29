package ayan.hadooprnd.mapreduce.custominputformat.edureka;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;




public class LogRecordInputFormat extends FileInputFormat<LogRecordKey,LogRecordValue> {
	
	
	@Override
	public RecordReader<LogRecordKey, LogRecordValue> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new LogRecordReader();
	}	
}
