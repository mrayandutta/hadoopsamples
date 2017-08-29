package ayan.hadooprnd.mapreduce.useraccess;

import ayan.hadooprnd.mapreduce.useraccess.UserActivityTrackerModern.MYCOUNTER;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserLogMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    private Text userKey = new Text();
    private Text valueText = new Text();
    
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
    {
    	/*
    	System.out.println("inside ..setup");
    	//Path[] cacheFilesLocal = DistributedCache.getLocalCacheArchives(context.getConfiguration());
    	//URI[] cacheFilesLocal = DistributedCache.getCacheFiles(context.getConfiguration());
    	Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	//FileInputStream fileStream = new FileInputStream(cacheFiles[0].toString());
 
		for (Path cachedFilePath : cacheFilesLocal) 
		{
			System.out.println("path::::"+cachedFilePath);
			readCacheFileContent(cachedFilePath);
		}
		*/
    	super.setup(context);
    }
    
    private void readCacheFileContent(Path cachedFilePath)
    {
    	Scanner scanner;
		try 
		{
			scanner = new Scanner(new File(cachedFilePath.toString()));
			scanner.useDelimiter(",");
			while(scanner.hasNext())
			{
		      System.out.print("record :"+scanner.next()+"|");
		    }
		    scanner.close();
		} 
		catch (FileNotFoundException e1) 
		{
			e1.printStackTrace();
		}
    }
    
    public void map(Text key, Text value,Context context) throws IOException, InterruptedException 
    {
    	context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
    	String line = value.toString();
        String[] inputArray = line.split("\\s+");
        if(inputArray!=null && inputArray.length==4)
        {
        	String timestamp = inputArray[0];
            String user = inputArray[1];
            String action = inputArray[2];
            String ip = inputArray[3];
            
            String keyStr = user.trim();
            String valueStr = action+","+timestamp+","+ip;
            
            //System.out.println("key:"+keyStr);
            //System.out.println("value:"+valueStr);
            userKey.set(keyStr);
            valueText.set(valueStr);
            context.write(userKey, valueText);
        }
        else
        {
        	System.out.println("Invalid Record !!!,inputArray:"+inputArray);
        }
    }
  }
