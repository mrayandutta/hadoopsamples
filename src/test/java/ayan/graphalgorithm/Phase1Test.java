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
import org.junit.Test;

public class Phase1Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	  public void setUp() {
	    Phase1Mapper phase1Mapper = new Phase1Mapper();
	    mapDriver = MapDriver.newMapDriver(phase1Mapper);
	    Phase1Reducer phase1Reducer = new Phase1Reducer();
	    reduceDriver = ReduceDriver.newReduceDriver(phase1Reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(phase1Mapper,phase1Reducer);
	  }
	
	//@Ignore
	@Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text("IP1 C1,C2"));
	    mapDriver.withOutput(new Text("C1"), new Text("IP1")).withOutput(new Text("C2"), new Text("IP1"));
	    mapDriver.runTest();
	  }
	
	//@Ignore
	@Test
	  public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
	    values.add(new Text("IP1"));
	    values.add(new Text("IP2"));
	    values.add(new Text("IP4"));
	    reduceDriver.withInput(new Text("C1"), values).
	    withOutput(new Text("IP1,IP2"), new Text("C1")).
	    withOutput(new Text("IP1,IP4"), new Text("C1")).
	    withOutput(new Text("IP2,IP4"), new Text("C1"));
	    boolean orderMatters = false;
	    reduceDriver.runTest(orderMatters);
	  }
	//@Ignore
	@Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text("IP1 C1,C3,C5")).
	    withInput(new LongWritable(), new Text("IP2 C1,C2,C3,C5")).
	    withInput(new LongWritable(), new Text("IP3 C2,C5")).
	    withOutput(new Text("IP1,IP2"), new Text("C1")).
	    withOutput(new Text("IP1,IP2"), new Text("C3")).
	    withOutput(new Text("IP1,IP2"), new Text("C5")).
	    withOutput(new Text("IP2,IP3"), new Text("C2")).
	    withOutput(new Text("IP2,IP3"), new Text("C5")).
	    withOutput(new Text("IP1,IP3"), new Text("C5"))
	    ;
	    boolean orderMatters = false;
	    mapReduceDriver.runTest(orderMatters);
	  }

}
