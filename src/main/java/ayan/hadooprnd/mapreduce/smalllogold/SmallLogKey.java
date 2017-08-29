package ayan.hadooprnd.mapreduce.smalllogold;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SmallLogKey implements WritableComparable<SmallLogKey> 
{
	private String user;
	private String value;
	
	/**
	 * Constructor.
	 */
	public SmallLogKey() { }
	
	/**
	 * Constructor.
	 * @param symbol Stock symbol. i.e. APPL
	 * @param timestamp Timestamp. i.e. the number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	public SmallLogKey(String user, String value) {
		this.user = user;
		this.value = value;
	}
	
	public void readFields(DataInput in) throws IOException {
		user = WritableUtils.readString(in);
		value = in.readLine();
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, user);
		out.writeChars(value);
	}

	public int compareTo(SmallLogKey o) {
		int result = user.compareTo(o.user);
		if(0 == result) {
			result = value.compareTo(o.value);
		}
		return result;
	}

	
}
