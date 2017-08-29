package ayan.hadooprnd.mapreduce.secondarysort;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Secondary sort reducer.
 * @author Jee Vang
 *
 */
public class SecondarySortReducer extends Reducer<StockKey, DoubleWritable, Text, Text> {

	private static final Log _log = LogFactory.getLog(SecondarySortReducer.class);
	
	@Override
	public void reduce(StockKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		Text k = new Text(key.toString());
		int count = 0;
		
		Iterator<DoubleWritable> it = values.iterator();
		while(it.hasNext()) {
			Text v = new Text(it.next().toString());
			context.write(k, v);
			_log.debug(k.toString() + " => " + v.toString());
			count++;
		}
		
		_log.debug("count = " + count);
	}
}
