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

public class Phase3Test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	  public void setUp() {
		Phase3Mapper phase3Mapper = new Phase3Mapper();
	    mapDriver = MapDriver.newMapDriver(phase3Mapper);
	    Phase3Reducer phase3Reducer = new Phase3Reducer();
	    reduceDriver = ReduceDriver.newReduceDriver(phase3Reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(phase3Mapper,phase3Reducer);
	  }
	
	@Ignore
	@Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(), new Text("C1 IP1,IP3"));
	    mapDriver.withInput(new LongWritable(), new Text("C2 IP1,IP2,IP3"));
	    
	    mapDriver.withOutput(new Text("IP1"), new Text("IP1,IP3,C1"));
	    mapDriver.withOutput(new Text("IP3"), new Text("IP1,IP3,C1"));
	    
	    mapDriver.withOutput(new Text("IP1"), new Text("IP1,IP2,C2"));
	    mapDriver.withOutput(new Text("IP1"), new Text("IP1,IP3,C2"));
	    mapDriver.withOutput(new Text("IP2"), new Text("IP1,IP2,C2"));
	    mapDriver.withOutput(new Text("IP2"), new Text("IP2,IP3,C2"));
	    mapDriver.withOutput(new Text("IP3"), new Text("IP1,IP3,C2"));
	    mapDriver.withOutput(new Text("IP3"), new Text("IP2,IP3,C2"));
	    boolean orderMatters = false;
	    mapDriver.runTest(orderMatters);
	  }
	@Ignore
	@Test
	  public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("IP1,IP3,C1"));
	    values.add(new Text("IP1,IP2,C2"));
	    values.add(new Text("IP1,IP3,C2"));
	    values.add(new Text("IP1,IP2,C3"));
	    
	    reduceDriver.withInput(new Text("IP1"), values).
	    withOutput(new Text("C1"), new Text("IP2")).withOutput(new Text("C3"), new Text("IP3"));
	    boolean orderMatters = false;
	    reduceDriver.runTest(orderMatters);
	  }
	
	@Test
	  public void testMapReduce() throws IOException {
	    mapReduceDriver.withInput(new LongWritable(), new Text("C1 IP1,IP3"));
	    mapReduceDriver.withInput(new LongWritable(), new Text("C2,C3 IP1,IP2,IP3"));
	    mapReduceDriver.
	    withOutput(new Text("C1"), new Text("IP2")).withOutput(new Text("C3"), new Text("IP3"));
	    boolean orderMatters = false;
	    mapReduceDriver.runTest(orderMatters);
	  }
	
}
