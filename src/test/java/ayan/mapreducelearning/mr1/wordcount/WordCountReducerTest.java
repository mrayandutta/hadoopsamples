package ayan.mapreducelearning.mr1.wordcount;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ayan.mapreducelearning.mr1.wordcount.WordCountDriver.WordCountReducer;

public class WordCountReducerTest {
	
private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setup(){
		WordCountReducer wordCountReducer = new WordCountReducer();
		reduceDriver = ReduceDriver.newReduceDriver(wordCountReducer);
	}
	
	@Test
	public void testSimpleReduce(){
		List<IntWritable> wordCountList = new ArrayList<IntWritable>();
		wordCountList.add(new IntWritable(1));
		wordCountList.add(new IntWritable(1));
		wordCountList.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("test"), wordCountList);
		reduceDriver.withOutput(new Text("test"), new IntWritable(3));
	}

}
