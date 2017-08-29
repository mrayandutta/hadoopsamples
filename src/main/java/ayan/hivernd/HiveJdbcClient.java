package ayan.hivernd;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	 
	  public static void main(String[] args) throws SQLException {
	    try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	      System.exit(1);
	    }
	    Connection con = DriverManager.getConnection("jdbc:hive://127.0.0.1:10000/default", "", "");
	    Statement stmt = con.createStatement();
	    ResultSet res = null;
	    // show tables
	    //String sql = "show tables";
	    String sql = "select * from t1";
	    System.out.println("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    // Create statement object for insert
	    Statement insertStmt = con.createStatement();
	    
	    while (res.next()) {
	    	System.out.println(res.getString(1)+"|"+res.getString(2)+"|"+res.getString(3));
	    	String ev = res.getString(1);
	    	String customer =res.getString(2);
	    	String transaction =res.getString(3);
	    	// Create SQL statement
		    String insertSQL = "INSERT INTO table t2 (ev, customer, transaction, c4) " +
		                 "VALUES('"+ev+"','"+customer+"','"+transaction+"','"+ev+transaction+" dummy "+"')";
		    System.out.println("insert sql :"+insertSQL);
		    // Add above SQL statement in the batch.
		    //insertStmt.addBatch(insertSQL);
		    insertStmt.execute(insertSQL);
	    	
	    }
	    
	    //insertStmt.executeBatch();
	 
	  }

}
