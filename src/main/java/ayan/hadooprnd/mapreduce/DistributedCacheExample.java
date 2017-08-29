package ayan.hadooprnd.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedCacheExample 
{
	public static String HDFS_URL="hdfs://10.0.2.15:8020";
	public static String HDFS_INPUT_LOCATION=HDFS_URL+File.separator+"data/dcinput";
	public static String CACHE_FILE_LOCATION=HDFS_URL+File.separator+"data/abc.dat";

	public static String HDFS_OUTPUT_LOCATION=HDFS_URL+File.separator+"dcoutput"+File.separator;
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> 
	{
		private Map<String, String> abMap = new HashMap<String, String>();
				private Text outputKey = new Text();
				private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			System.out.println("setup() invoked !!!");
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			for (Path p : files) {
				if (p.getName().equals("abc.dat")) 
				{
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String ab = tokens[0];
						String state = tokens[1];
						abMap.put(ab, state);
						line = reader.readLine();
						System.out.println("ab:"+ab);
					}
				}
			}
			if (abMap.isEmpty()) 
			{
				throw new IOException("Unable to load Abbrevation data.");
			}
		}

		
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        	
        	System.out.println("map() invoked !!!");
    		
        	String row = value.toString();
        	String[] tokens = row.split("\t");
        	String inab = tokens[0];
        	String state = abMap.get(inab);
        	outputKey.set(state);
        	outputValue.set(row);
      	  	context.write(outputKey,outputValue);
        }  
}
	
	
  public static void main(String[] args) 
                  throws IOException, ClassNotFoundException, InterruptedException {
    
    Job job = new Job();
    job.setJarByClass(DistributedCacheExample.class);
    job.setJobName("DistributedCacheExample");
    job.setNumReduceTasks(0);
    
    try
    {
    	//DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
    	DistributedCache.addCacheFile(new URI(CACHE_FILE_LOCATION), job.getConfiguration());
    }
    catch(Exception e)
    {
    	System.out.println(e);
    }
    
    job.setMapperClass(MyMapper.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path(HDFS_INPUT_LOCATION));
    FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUT_LOCATION));
    job.waitForCompletion(true);
    
    
  }

}
