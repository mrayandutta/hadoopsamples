package ayan.hadooprnd.mapreduce.smalllogold;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class DriverCombineFileInputFormat {
 
  public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf("DriverCombineFileInputFormat");
		//conf.set("mapred.max.split.size", "134217728");//128 MB
		conf.set("mapred.max.split.size", "67108864");//64 MB
		conf.setJarByClass(DriverCombineFileInputFormat.class);
		String[] jobArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
 
		conf.setMapperClass(MapperCombineFileInputFormat.class);
		conf.setInputFormat(ExtendedCombineFileInputFormat.class);
		ExtendedCombineFileInputFormat.addInputPath(conf, new Path(jobArgs[0]));
 
		conf.setNumReduceTasks(0);
 
		conf.setOutputFormat(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(conf, new Path(jobArgs[1]));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
 
		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			Thread.sleep(1000);
		}
 
		System.exit(job.isSuccessful() ? 0 : 2);
	}
}
