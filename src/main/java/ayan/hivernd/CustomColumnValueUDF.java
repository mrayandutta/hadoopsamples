package ayan.hivernd;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
 

public final class CustomColumnValueUDF extends UDF {
  public Text evaluate(final Text input) {
	  String inputStr = input.toString();
	  Text output = new Text(inputStr+" changed !!");
	  return output;
  }
}
