package ayan.loginsight.logprocessorwithmop;
  
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
  
public class LogInsightMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    private Text userKey = new Text();
    private Text valueText = new Text();
    // MultipleOutputs will be used to write different output files based on input file directory
    private MultipleOutputs mos;
    // MultipleOutputs would use following outputPathKey as file output file name
    private String outputPathKey = "defaultoutput";
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException 
    {
     //Instantiate MultipleOutputs
     mos  = new MultipleOutputs(context);
     //The split is not FileSplit,it is CombineFileSplit as CustomCombinedInputFormat is used
     CombineFileSplit split = (CombineFileSplit) context.getInputSplit();
     //CombineFileSplit contains an array of Paths.If it was FileSplit,it would have returned single path
     //using the getPath() method
     Path[] paths = split.getPaths();
     //Based on the input path the outputPathKey value is assigned
	     for (int i = 0; i < paths.length; i++) 
	     {
	    	 if(paths[i].toString().contains("conjoint"))
	    	 {
	    		 outputPathKey="conjoint";
	    		 break;
	    	 }
	    	 else
	    	 {
	    		 if(paths[i].toString().contains("starttrack"))
	        	 {
	        		 outputPathKey="startrack";
	        		 break;
	        	 }
	    	 }
		}
    }
     
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException 
    {
    	if(outputPathKey.equalsIgnoreCase("starttrack"))
    	{
    		processStartrackLogs(key, value, context);
    	}
    	else
    	{
    		processConjointLogs(key, value, context);
    	}
    }
    
    private void processStartrackLogs(LongWritable key, Text value,Context context) throws IOException, InterruptedException
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
                String keyStr = user.trim();
                String valueStr = reportName+","+reportExecutionTime+","+timestampStr+","+ip;
                userKey.set(keyStr);
                valueText.set(valueStr);
                //context.write(userKey,valueText);
                mos.write(userKey, valueText, outputPathKey);
        } 
        else
        {
            System.out.println("Invalid Record !!!,inputArray:"+line);
        }
    
    }
    
    private void processConjointLogs(LongWritable key, Text value,Context context) throws IOException, InterruptedException
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
                String keyStr = user.trim();
                String valueStr = reportName+","+reportExecutionTime+","+timestampStr+","+ip;
                userKey.set(keyStr);
                valueText.set(valueStr);
                //context.write(userKey,valueText);
                mos.write(userKey, valueText, outputPathKey);
                System.out.println("timestamp:"+timestamp);
                System.out.println("user:"+user);
        } 
        else
        {
            System.out.println("Invalid Record !!!,inputArray:"+line);
        }
    
    }
    
    @Override
    protected void cleanup(Context context)throws IOException, InterruptedException 
    {
    	mos.close();
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