package ayan.mapreducelearning.mr1.wordcount;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import ayan.mapreducelearning.mr1.wordcount.WordCountDriver.WordCountMapper;
import ayan.mapreducelearning.mr1.wordcount.WordCountDriver.WordCountReducer;

public class WordCountMRTest {
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		WordCountMapper mapper = new WordCountMapper();
		WordCountReducer reducer = new WordCountReducer();

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testSimple() throws Exception {
		mapReduceDriver.withInput(new LongWritable(1), new Text(
				"This test is simple test"));
		List<Pair<Text, IntWritable>> outputs = new ArrayList<Pair<Text, IntWritable>>();
		outputs.add(new Pair<Text, IntWritable>(new Text("This"),
				new IntWritable(1)));
		outputs.add(new Pair<Text, IntWritable>(new Text("is"),
				new IntWritable(1)));
		outputs.add(new Pair<Text, IntWritable>(new Text("simple"),
				new IntWritable(1)));
		outputs.add(new Pair<Text, IntWritable>(new Text("test"),
				new IntWritable(2)));
		mapReduceDriver.withAllOutput(outputs);
		mapReduceDriver.runTest();
	}
}
