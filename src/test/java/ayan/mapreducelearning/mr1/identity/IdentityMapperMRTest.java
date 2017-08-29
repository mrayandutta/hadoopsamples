package ayan.mapreducelearning.mr1.identity;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class IdentityMapperMRTest 
{
	private Mapper mapper;
	private MapDriver mapDriver;
	
	private Reducer reducer;
	private ReduceDriver reduceDriver;
	 
	 @Before
	 public void setUp()
	 {
	  mapper = new IdentityMapper();
	  mapDriver = new MapDriver(mapper);
	  
	  reducer = new IdentityReducer();
	  reduceDriver = new ReduceDriver(reducer);
	 }
	 
	 @Test
	 public void testIdentityMapper() throws IOException
	 {
		mapDriver.withInput(new Text("key"), new Text("value"))
	   .withOutput(new Text("key"), new Text("value"))
	   .runTest();
	 }
	 
	 /*
	 @Test
	 public void testIdentityReducer() throws IOException{
		 reduceDriver.withInput(new Text("key"), new Text("value"))
	   .withOutput(new Text("key"), new Text("value"))
	   .runTest();
	 }
	 */
	 
	 

}
