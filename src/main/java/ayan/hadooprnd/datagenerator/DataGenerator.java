package ayan.hadooprnd.datagenerator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator 
{
	public static String ELECTION_LOG_FILE_NAME=File.separator+"home"+File.separator+"user"+File.separator+"Desktop"+File.separator+"input"+File.separator+"electionlog.txt";
	public static int SIZE_IN_MB=50;
	public static int SIZE_IN_KB=10*SIZE_IN_MB;
	//public static int SIZE_IN_KB=1;
	public static int LINE_COUNT=1*SIZE_IN_KB;

	
	public static void main(String[] args) 
	{
		for (int i = 0; i < LINE_COUNT; i++) 
		{
			addContentToFile(i,i,ELECTION_LOG_FILE_NAME);
		}
		
		System.out.println("Data file created at :"+ELECTION_LOG_FILE_NAME);
	}
	
	public static void addContentToFile(int count,int noOfCandidate,String fileLocation)
	{
		try 
		{
			String content = getElectionData(count,noOfCandidate);
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

	
	public static String getElectionData(double count,int noOfCandidate)
	{
		StringBuffer sb = new StringBuffer();
		Random r = new Random();
		int low = 1;
		int high = noOfCandidate;
		
		for (int i = 0; i < count; i++) 
		{
			
			for (int j = 1; j < noOfCandidate; j++) 
			{
				int randomVoteCount = r.nextInt(high-low) + low;
				String electionInfoLine ="C"+j+" "+randomVoteCount;
				sb.append(electionInfoLine);
				sb.append("\n");
			}
		}
		String dataStr=sb.toString();
		return dataStr;
	}

}
