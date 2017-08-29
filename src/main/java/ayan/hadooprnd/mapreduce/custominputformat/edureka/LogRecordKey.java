package ayan.hadooprnd.mapreduce.custominputformat.edureka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class LogRecordKey implements WritableComparable{
	private Text sensorType,timestamp,status;
	
	public LogRecordKey(){
		this.sensorType = new Text();
		this.timestamp = new Text();
		this.status = new Text();
	}
	public LogRecordKey(Text sensorType,Text timestamp,Text status){
		this.sensorType = sensorType;
		this.timestamp = timestamp;
		this.status = status;		
	}
	public void readFields(DataInput in) throws IOException{
		sensorType.readFields(in);
		timestamp.readFields(in);
		status.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException{
		sensorType.write(out);
		timestamp.write(out);
		status.write(out);
	}
	public int compareTo(Object o){
		LogRecordKey other = (LogRecordKey)o;
		int cmp = sensorType.compareTo(other.sensorType);
		if(cmp != 0){
				return cmp;
		}
		cmp = timestamp.compareTo(other.timestamp);
		if(cmp != 0){
				return cmp;
		}
		return status.compareTo(other.status);
		
	}
	public Text getsensorType() {
		return sensorType;
	}
	public void setsensorType(Text sensorType) {
		sensorType = sensorType;
	}
	public Text getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Text timestamp) {
		this.timestamp = timestamp;
	}
	public Text getStatus() {
		return status;
	}
	public void setStatus(Text status) {
		this.status = status;
	}
	

}
