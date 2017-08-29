package ayan.hadooprnd.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Secondary sort mapper.
 * @author Jee Vang
 *
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, StockKey, DoubleWritable> {

	private static final Log _log = LogFactory.getLog(SecondarySortMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		
		String symbol = tokens[0].trim();
		Long timestamp = Long.parseLong(tokens[1].trim());
		Double v = Double.parseDouble(tokens[2].trim());
		
		StockKey stockKey = new StockKey(symbol, timestamp);
		DoubleWritable stockValue = new DoubleWritable(v);
		
		context.write(stockKey, stockValue);
		_log.debug(stockKey.toString() + " => " + stockValue.toString());
	}
}
