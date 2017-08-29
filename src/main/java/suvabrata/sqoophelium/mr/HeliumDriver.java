package suvabrata.sqoophelium.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HeliumDriver 
{

	public static void main(String[] args) throws Exception 
	{
		System.out.println("Project Helium:- Healing the Sqoop Ingested Data");
		System.out.println("Description:- Removes InvalidAsciiCharacters");
		System.out
				.println("USAGE: hadoop jar HeliumHealer com.helium.HeliumDriver "
						+ "<hdfs_input_path> <hdfs_output_path> <job_name> <escapeCharacterAsciiCode>");

		Configuration conf = new Configuration();
		String job_name = "helium_burning_" + args[2];
		conf.set("escapeCharacterAsciiCode",args[3]);
		Job job = Job.getInstance(conf, job_name);		
		job.setJarByClass(HeliumDriver.class);
		job.setMapperClass(suvabrata.sqoophelium.mr.HeliumMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
