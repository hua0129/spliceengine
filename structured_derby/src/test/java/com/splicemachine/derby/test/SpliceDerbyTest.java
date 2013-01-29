package com.splicemachine.derby.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Ignore;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

@Ignore
public class SpliceDerbyTest {
	private static final Logger LOG = Logger.getLogger(SpliceDerbyTest.class);
    protected static String framework = "embedded";
    protected static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    protected static String protocol = "jdbc:derby:splice:";
    protected static Properties props = new Properties();
	protected static final String dbName = "wombat"; // gd for derby test compatibility
	protected static Connection conn = null;
	protected static List<Statement> statements = new ArrayList<Statement>();
	
    protected static void loadDriver() {
        try {
            Class.forName(driver).newInstance();
            System.out.println("Loaded the appropriate driver");
        } catch (ClassNotFoundException cnfe) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            cnfe.printStackTrace(System.err);
        } catch (InstantiationException ie) {
            System.err.println(
                        "\nUnable to instantiate the JDBC driver " + driver);
            ie.printStackTrace(System.err);
        } catch (IllegalAccessException iae) {
            System.err.println(
                        "\nNot allowed to access the JDBC driver " + driver);
            iae.printStackTrace(System.err);
        }
    }
    
    protected static void printSQLException(SQLException e) {
        while (e != null)
        {
            System.err.println("\n----- SQLException -----");
            System.err.println("  SQL State:  " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            e = e.getNextException();
        }
    }

    public static void splitLastTable() throws Exception {
    	//get the conglomerate ID from ZooKeeper
		long conglomID = SpliceUtils.getHighestConglomSequence();
		HBaseAdmin admin = new HBaseAdmin(SpliceUtils.config);
		
		byte[] nameBytes = SpliceAccessManager.getHTable(conglomID).getTableName();
		String name = Bytes.toString(nameBytes);
		SpliceLogUtils.trace(LOG,"table id=%s",name);
		List<HRegionInfo> existingRegions = admin.getTableRegions(nameBytes);
		SpliceLogUtils.trace(LOG, "table %s existing regions=%s",name,existingRegions);
		//split the HBase table for this id into two regions
		admin.split(name);
		List<HRegionInfo> newRegions = admin.getTableRegions(nameBytes);
		SpliceLogUtils.trace(LOG, "table %s new regions=%s",name,newRegions);
    }
    
    public static void splitTable(String tableName) throws SQLException {
    	Statement s = null;
    	ResultSet rs = null;
    	try{
    		rs = executeQuery("select * from sys.sysconglomerates ");
    		while(rs.next()){
    			System.out.println(rs.getString("conglomeratename"));
    		}
    	}finally{
    		if (rs != null)
    			rs.close();
    		if (s !=null)
    			s.close();
    	}
    }

	public static void startConnection() throws SQLException {
		loadDriver();
		try {
			conn = DriverManager.getConnection(protocol + dbName + ";create=true", props);
			System.out.println("Conn: " + conn);
		}
		catch (Exception e) {
			System.out.println("Exception: " + e + ", conn: " + conn);
			e.printStackTrace();
		}
	}
	
	public static void stopConnection() throws SQLException {
	    //Connection
	    try {
	        if (conn != null) {
	            conn.close();
	            conn = null;
	        }
	    } catch (SQLException sqle) {
	        printSQLException(sqle);
	    }		
	}

    
	public static ResultSet executeQuery (String sql) {
        try {
        	Statement s = conn.createStatement();
        	statements.add(s);
        	return s.executeQuery(sql);
        } catch (SQLException sqle) {
        	sqle.printStackTrace();
            printSQLException(sqle);
        }
        return null;
}

	public static void executeStatement (String sql) throws SQLException {
		Statement s = null;
        try {
            s = conn.createStatement();
            statements.add(s);
            s.execute(sql);
        } catch (SQLException sqle) {
        	sqle.printStackTrace();
            printSQLException(sqle);
        } finally {
        	if (s!= null)
        		s.close();
        }
     }

	protected static void closeStatements () throws SQLException {
		for (Statement statement: statements) 
			statement.close();
     }

	protected static void dropTable(String tableName) throws SQLException {	
		Statement s = null;
		try {
			conn.setAutoCommit(true);
			s = conn.createStatement();
			s.execute("drop table "+tableName);
		} catch (SQLException e) {
			LOG.error("error on drop table-"+tableName+": "+e.getMessage(), e);
		} finally {
			try {
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}		
	}
}
