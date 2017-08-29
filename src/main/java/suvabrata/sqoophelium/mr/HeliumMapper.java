package suvabrata.sqoophelium.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HeliumMapper extends Mapper<LongWritable, Text, Text, Text> {

	public List<String> listInvalidCharacters = new ArrayList<String>();
	/*
	 * Declaring List to hold the Invalid ASCII Characters
	 */

	public void initializeInvalidCharacters(int escapeCharacterAsciiCode) 
	{
		int i = 0;
		int j = 127;
		//Inserting Non Printable ASCII 0-31 to the List
		for (i = 0; i <= 31; i++) 
		{
			String invalidChar = Character.toString((char) i);
			listInvalidCharacters.add(invalidChar);
		}
		listInvalidCharacters.remove(escapeCharacterAsciiCode);
		//removing the line feed ASCII(\n) Value from the List
		listInvalidCharacters.remove(10);
		//Adding ASCII 127 to the List
		listInvalidCharacters.add(Character.toString((char) j));
	}

	public String removeInvalidCharsFromString(String input_line) 
	{
		String output_line = input_line;
		for (String invalidAsciiChar : listInvalidCharacters) 
		{
			output_line = output_line.replaceAll(invalidAsciiChar, "");
		}
		return output_line;
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		int escapeCharacterAsciiCode = Integer.parseInt(context.getConfiguration().get("escapeCharacterAsciiCode"));
		initializeInvalidCharacters(escapeCharacterAsciiCode);

		String line = value.toString();
		String filteredLine = removeInvalidCharsFromString(line);
		context.write(new Text(filteredLine), new Text(""));
	}
}
