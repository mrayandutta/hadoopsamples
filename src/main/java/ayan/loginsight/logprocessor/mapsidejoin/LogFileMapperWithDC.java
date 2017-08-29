package ayan.loginsight.logprocessor.mapsidejoin;
  
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
  
public class LogFileMapperWithDC extends Mapper<LongWritable, Text, Text, Text> 
  {
	//This map would store the userid,user name key value pair extracted from the * distributed cache .txt file. 
	private static HashMap<String, String> hvcMap = new HashMap<String, String>();
	private final static Logger logger = Logger.getLogger(LogFileMapperWithDC.class);
	private Text userKey = new Text();
    private Text valueText = new Text();
    
    @Override
	protected void setup(Context context) throws IOException,InterruptedException {
    	/*
    	 * Get the Path array of files store in the Distributed Cache
    	 */
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		/*
		 * Check which path contains the specific file name and send that path to
		 * populateUserMap() function
		 */
		for (Path eachPath : cacheFilesLocal) 
		{
			if (eachPath.getName().toString().trim().equals("highlyvaluedcustomer.txt")) {
				populateUserMap(eachPath);
			}
		}
		
		logger.info("Inside setup");
	}
    
    /**
     * This method populates the hvcMap collection from .txt file 
     * @param filePath
     * @throws IOException
     */
    private void populateUserMap(Path filePath) throws IOException 
    {
		String strLineRead = "";
		BufferedReader brReader = null;
		try 
		{
			brReader = new BufferedReader(new FileReader(filePath.toString()));
			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) 
			{
				String userFieldArray[] = strLineRead.split(",");
				String userId = userFieldArray[0].trim();
				String userName = userFieldArray[1].trim();
				hvcMap.put(userId,userName);
			}
		} catch (Exception e) 
		{e.printStackTrace();} 
		finally 
		{
			if (brReader != null) {brReader.close();}
 
		}
		logger.info("Highly Valued Customer Map :"+hvcMap);
	}
     
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        //Sample record 21-10-2085:09:56:02 User3 Report2 32 192.168.04.01
        String logEntryPattern ="(\\d{2}-\\d{2}-\\d{4}:\\d{2}:\\d{2}:\\d{2})\\s(\\w+)\\s(\\w+)\\s(\\w+)"
                + "\\s(\\d{3}.\\d{3}.\\d{2}.\\d{2})";
        Pattern pattern = Pattern.compile(logEntryPattern);
        Matcher m = pattern.matcher(line);
        if (m.find( ) && m.groupCount()==5) 
        {
                String timestamp = m.group(1);
                String timestampStr = getDayMonthYearStrFromTS(timestamp);
                 
                String user = m.group(2);
                String reportName = m.group(3);
                String reportExecutionTime = m.group(4);
                String ip = m.group(5);
                String valueStr = reportName+","+reportExecutionTime+","+timestampStr+","+ip;
                valueText.set(valueStr);
                String userkeyStr = user.trim();
                userKey.set(userkeyStr);
                //Finally check whether the user is a highly values user/customer or not
                if(hvcMap.containsKey(userkeyStr))
                {
                	context.write(userKey,valueText);
                }
                else
                {
                	logger.info("This record is filtered as "+userkeyStr +" is not present in "+hvcMap);
                }
        } 
        else
        {
            logger.info("Invalid Record !!!,inputArray:"+line);
        }
    }
     
    private String getDayMonthYearStrFromTS(String inputDateStr)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy:hh:mm:ss");
        String dateStr ="";
        try
        {
            Date date = sdf.parse(inputDateStr);
            Calendar calendar =Calendar.getInstance();
            calendar.setTimeInMillis(date.getTime());
             
            dateStr =calendar.get(Calendar.DAY_OF_MONTH)+","+(calendar.get(Calendar.MONTH)+1)+","+calendar.get(Calendar.YEAR);
        } 
        catch (ParseException ex) 
        {
            ex.printStackTrace();
        }
        return dateStr;
    }
  }