package ayan.mapreducelearning.mr1.wordcount;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ASCIIRemoverUtil 
{
	private static List<String> invalidAsciiList = new ArrayList<String>();
	static
	{
		for (int i=0;i<=26;i++)
		{
			String  asciiCharToBeFiltered= Character.toString((char)i);
			invalidAsciiList.add(asciiCharToBeFiltered);
		}
		
		System.out.println("ascii list :"+invalidAsciiList);
		invalidAsciiList.remove(10);
	}
	
	public static String removeInvalidCharsFromString(String input)
	{
		String output = input;
		for (String invalidAsciiChar: invalidAsciiList) 
		{
			output = output.replaceAll(invalidAsciiChar, " ");
		}
		return output;
	}

	public static String getTextFromFile(String fileLocation)
	{
		StringBuffer sb = new StringBuffer();
		try {
            
            File file = new File(fileLocation);
            Scanner input = new Scanner(file);
            input = new Scanner(file);


            while (input.hasNextLine()) {
                String line = input.nextLine();
                for (int i = 0; i < line.length(); i++) {
					char ch = line.charAt(i);
					int intValue = (int)ch;
					if(intValue==2)
					{
						System.out.println("intValue:"+intValue +",ch:"+ch+",ch2:"+(char)2);
					}
				}
                sb.append(line);
                sb.append("\n");
              
            }
            input.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
		return sb.toString();
	}
	public static void writeTextToFile(String text ,String fileLocation)
	{
		BufferedWriter writer = null;
		try {
            
            File file = new File(fileLocation);


            writer = new BufferedWriter(new FileWriter(file));
            writer.write(text);

		} catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }
	}

	public static void main(String[] args) {
		String inputFileLocation = "/home/user/Desktop/data/data1.txt";
		String outputFileLocation = "/home/user/Desktop/data/output.txt";
		//Get the raw text from file 
		String input=getTextFromFile(inputFileLocation);
		//Remove invalid characters
		String filteredText = removeInvalidCharsFromString(input);
		//Store the changed text in local drive
		
		writeTextToFile(filteredText, outputFileLocation);
			
		}
}
