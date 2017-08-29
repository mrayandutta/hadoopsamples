package ayan.hadooprnd.mapreduce.useraccess;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class WeblogUtil
{
	public static String LOG_FILE_DIRECTORY=File.separator+"home"+File.separator+"user"+File.separator+"Desktop"+File.separator+"logs";
	public static String LOG_FILE_NAME=LOG_FILE_DIRECTORY+File.separator+"weblog.txt";
	
	public static int SIZE_IN_MB=5;
	//public static int SIZE_IN_KB=1000*SIZE_IN_MB;
	public static int SIZE_IN_KB=1;
	public static int LINE_COUNT=5*SIZE_IN_KB;
	public static int USER_COUNT=5;
	

	public static String USER="user";
	public static String KPI="KPI";
	public static String SESSION_ID="SESSION_ID";
	public static String ACTION="action";
	public static String IP="IP";
	public static Date  timestamp=new Date();
	
	public static String SPACE=" ";
	public static String NEWLINE="\n";
	
	public static String getUserString(int userCount)
	{
		Random r = new Random();
		int low = 1;
		int high = userCount;
		int randomNumber = r.nextInt(high-low) + low;
		//System.out.println("low:"+low+",high:"+high+",randomNumber:"+randomNumber);
		String userStr = "User"+randomNumber;
		//System.out.println("userStr:"+userStr);
		return userStr;
	}
	
	public static String getRandomKPI(int kpiHighestValue)
	{
		Random r = new Random();
		int low = 1;
		int high = kpiHighestValue;
		int randomNumber = r.nextInt(high-low) + low;
		String kpiStr = ""+randomNumber;
		return kpiStr;
	}
	
	public static String createDataString(int index)
	{
		StringBuffer sb = new StringBuffer();
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy:hh:mm:ss");
		for (int i = 0; i < LINE_COUNT; i++) 
		{
			timestamp = new Date(timestamp.getTime()+i*1000);
			String timestampStr = sdf.format(timestamp);
			sb.append(timestampStr+SPACE+getUserString(USER_COUNT)+SPACE+ACTION+i+SPACE+IP+index+NEWLINE);
			
		}
		String dataStr=sb.toString();
		return dataStr;
	}
	
	public static void addContentToFile(int index,String fileLocation)
	{
		try 
		{
			String content = createDataString(index);
			File file = new File(fileLocation);
 
			// if file doesnt exists, then create it
			if (file.exists()) 
			{
				FileWriter fw = new FileWriter(fileLocation,true); //the true will append the new data
			    fw.write(content);//appends the string to the file
			    fw.close();
			}
			else
			{
				file.createNewFile();
				FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(content);
				bw.close();
			}
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void createSingleLogFile(String fileLocation)
	{
		for (int i = 0; i < USER_COUNT; i++) 
		{
			addContentToFile(i,fileLocation);
		}
	}
	public static void createMultipleLogFile(int fileCount)
	{
		for (int i = 1; i <= fileCount; i++) 
		{
			String fileName ="weblog"+i;
			String fileLocation = LOG_FILE_DIRECTORY+File.separator+fileName;
			createSingleLogFile(fileLocation);
		}
	}

	public static void main(String[] args) 
	{
		int noOfLogFiles=50;
		createSingleLogFile(LOG_FILE_NAME);
		//createMultipleLogFile(noOfLogFiles);
		System.out.println("Done");
	}

}
