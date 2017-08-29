package ayan.hadooprnd.mapreduce.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionMapper extends Mapper<Object, Text, Text, Text>
{
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException 
	{
		 String[] tokens = value.toString().split("\t");
		 
         String gender = tokens[2].toString();
         String nameAgeScore = tokens[0]+"\t"+tokens[1]+"\t"+tokens[3];
        
         //the mapper emits key, value pair where the key is the gender and the value is the other information which includes name, age and score
         context.write(new Text(gender), new Text(nameAgeScore));
	}
}
