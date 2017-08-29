package ayan.graphalgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class IntegrationTest {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	private Mapper<LongWritable, Text, Text, Text> mapper1;
	private Reducer<Text, Text, Text, Text> reducer1;
	private Mapper<LongWritable, Text, Text, Text> mapper2;
	private Reducer<Text, Text, Text, Text> reducer2;
	private PipelineMapReduceDriver<LongWritable, Text, Text, Text> driver;
	
	@Before
	  public void setUp() {
		mapper1 = new Phase1Mapper();
		reducer1 = new Phase1Reducer();
		mapper2 = new Phase2Mapper();
		reducer2 = new Phase2Reducer();
		
		driver = new PipelineMapReduceDriver<LongWritable, Text, Text, Text>();
		//driver.withMapReduce(mapper1, reducer1);
		//driver.addMapReduce(new Pair<Mapper, Reducer>(mapper1, reducer1));
		//driver.addMapReduce(new Pair<Mapper, Reducer>(mapper2, reducer2));
	  }
	
	//@Ignore
	@Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text("IP1,IP2 C1,C2"));
	    mapDriver.withOutput(new Text("IP1,IP2"), new Text("C1,C2"));
	    mapDriver.runTest();
	  }
	
	//@Ignore
	@Test
	  public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
	    values.add(new Text("C1"));
	    values.add(new Text("C2"));
	    reduceDriver.withInput(new Text("IP1,IP2"), values).
	    withOutput(new Text("IP1,IP2"), new Text("C1,C2"));
	    boolean orderMatters = false;
	    reduceDriver.runTest(orderMatters);
	  }
	//@Ignore
	@Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text("IP1,IP2 C1,C2")).
	    withInput(new LongWritable(), new Text("IP1,IP2 C3")).
	    withOutput(new Text("IP1,IP2"), new Text("C1,C2,C3"));
	    boolean orderMatters = false;
	    mapReduceDriver.runTest(orderMatters);
	  }

}
