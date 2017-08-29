package ayan.graphalgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class Phase2Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	  public void setUp() {
		Phase2Mapper phase2Mapper = new Phase2Mapper();
	    mapDriver = MapDriver.newMapDriver(phase2Mapper);
	    Phase2Reducer phase2Reducer = new Phase2Reducer();
	    reduceDriver = ReduceDriver.newReduceDriver(phase2Reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(phase2Mapper,phase2Reducer);
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
