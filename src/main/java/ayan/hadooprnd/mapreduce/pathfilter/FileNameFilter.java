package ayan.hadooprnd.mapreduce.pathfilter;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FileNameFilter extends Configured implements PathFilter
{

	public boolean accept(Path path) 
	{
		boolean accept =false;
		System.out.println("File path :"+path.toString());
		return accept;
	
	}

}
