/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.log4j.Logger;

public class HiveConnectionManager {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Logger logger = Logger.getLogger(HiveConnectionManager.class);

    //TODO: No need to specify full path. Maybe pass JAR_PATH to Manager
    //TODO: Apply HIVE-1414 so this becomes pre-loaded to Hive
    private final static String JAR_PATH = "/usr/lib/hive/lib/";
    private final static String MYSQL_JAR = JAR_PATH + "mysql-connector-java-5.1.15-bin.jar";
    private final static String CONTRIB_JAR = JAR_PATH + "hive-contrib-0.7.1-cdh3u1.jar";
    private final static String DAY_JAR = JAR_PATH + "Day.jar";
    private final static String DAYOFWEEK_JAR = JAR_PATH + "DayOfWeek.jar";
    private final static String MONTH_JAR = JAR_PATH + "Month.jar";
    private final static String HOUR_JAR = JAR_PATH + "Hour.jar";
    private final static String NULL_JAR = JAR_PATH + "NullToZero.jar";

	private Connection conn;
	private String dbName;
	private String port;
	private String serverAddr;
	
	public HiveConnectionManager() throws HiveDAOException{
		this("hivedev1", "10000", "default");
	}
	
	public HiveConnectionManager(final String server, String port, final String dbName) throws HiveDAOException {
		if(server == null || port == null || dbName == null)
	      throw new IllegalArgumentException(String.format("Parameters can't " +
	      		"be null: server=%s, port=%s, dbName=%s", server, port, dbName));
        this.serverAddr = server;
		this.port = port;
		this.dbName = dbName;
		conn = createConnection(server,port,dbName);
	}
	
	public Connection createConnection(final String server, String port, final String dbName) throws HiveDAOException {
		String connStr = "";
		try{
			Class.forName(driverName);
			connStr = "jdbc:hive://" + server + ":" + port + "/" + dbName;
			conn = DriverManager.getConnection(connStr);
            conn = openDatabase();
            conn = addJar(MYSQL_JAR);
            conn = addJar(CONTRIB_JAR);
            conn = addJar(DAY_JAR);
            conn = addJar(DAYOFWEEK_JAR);
            conn = addJar(MONTH_JAR);
            conn = addJar(HOUR_JAR);
            conn = addJar(NULL_JAR);
            conn = addUDF("org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput","dboutput");
            conn = addUDF("com.smithmicro.hive.udf.Day","Day");
            conn = addUDF("com.smithmicro.hive.udf.DayOfWeek","DayOfWeek");
            conn = addUDF("com.smithmicro.hive.udf.Month","Month");
            conn = addUDF("com.smithmicro.hive.udf.Hour","Hour");
            conn = addUDF("com.smithmicro.hive.udf.NullToZero","NullToZero");
		}catch( ClassNotFoundException e){
			logger.error("Hive JDBC driver "+driverName+" not found on class path.",e );
			throw new HiveDAOException(e,"Hive JDBC driver "+driverName+" not found on class path.");
		}catch (SQLException e) {
			logger.error("Unable to establish connection to: "+connStr,e);
			throw new HiveDAOException(e,"Unable to establish connection to: "+connStr);
		}
		return conn;
	}

    public Connection openDatabase() throws HiveDAOException {
        if( conn == null ){
			throw new HiveDAOException(new NullPointerException("java.sql.Connection conn is null"),"sql connection is null, can not create Statement");
		}
        try{
            Statement stmt = createStatement();
            StringBuffer sql = new StringBuffer("use ");
            sql.append(dbName);
            stmt.execute(sql.toString());
		}catch(SQLException e){
			logger.error("unable to open database",e);
			throw new HiveDAOException(e,"unable to open database");
		}
        return conn;
    }


    /**
    * Loads a Jar into Hive
    * @param jarPath absolute path can be used if jar is not in default location
    * @return Connection
    * @throws HiveDAOException
    */
    public Connection addJar(final String jarPath) throws HiveDAOException {
        if( conn == null ){
			throw new HiveDAOException(new NullPointerException("java.sql.Connection conn is null"),"sql connection is null, can not create Statement");
		}
        try{
            //Add Jar
            Statement stmt = createStatement();
            StringBuffer sql = new StringBuffer("add jar ");
            sql.append(jarPath);
            Boolean res = stmt.execute(sql.toString());
            //System.out.println(res.toString());
		}catch(SQLException e){
			logger.error("unable to add jar",e);
			throw new HiveDAOException(e,"unable to add jar");
		}
        return conn;
    }

    /**
    * Loads a User Defined Function (UDF) into Hive
    * @param jarPath absolute path can be used if jar is not in default location
    * @param className name
    * @param functionName what you would like to name the loaded function
    * @return Connection
    * @throws HiveDAOException
    */
    public Connection addUDF(final String jarPath, final String className, final String functionName) throws HiveDAOException {
        if( conn == null ){
			throw new HiveDAOException(new NullPointerException("java.sql.Connection conn is null"),"sql connection is null, can not create Statement");
		}
        //Add Jar
        addJar(jarPath);
        //Create Function
        addUDF(className,functionName);
        return conn;
    }
    public Connection addUDF(final String className, final String functionName) throws HiveDAOException {
        if( conn == null ){
			throw new HiveDAOException(new NullPointerException("java.sql.Connection conn is null"),"sql connection is null, can not create Statement");
		}
        try{
            //Create Function
            Statement stmt = createStatement();
            stmt = createStatement();
            StringBuffer sql = new StringBuffer("create temporary function ");
            sql.append(functionName);
            sql.append(" as ");
            sql.append("'");
            sql.append(className);
            sql.append("'");
            stmt.execute(sql.toString());
		}catch(SQLException e){
			logger.error("unable to load/add udf",e);
			throw new HiveDAOException(e,"unable to load/add udf");
		}
        return conn;
    }
	
	public Statement createStatement() throws HiveDAOException {
		if( conn == null ){
			throw new HiveDAOException(new NullPointerException("java.sql.Connection conn is null"),"sql connection is null, can not create Statement");
		}
		try{
			return conn.createStatement();
		}catch(SQLException e){
			logger.error("unable to create statement",e);
			throw new HiveDAOException(e,"unable to create statement");
		}
	}
	public Connection getConnection(){
		return this.conn;
	}
	
	public String getDatabaseName(){
		return this.dbName;
	}
	
	public String getServerAddress(){
		return this.serverAddr;
	}
	
	public String getPort(){
		return this.port;
	}
	
	public static void closeConnections(Connection conn, Statement stmt, ResultSet rs){
		if( rs != null ){
			try{
				rs.close();
			}catch(Exception e){}
		}
		if( stmt != null ){
			try{
				stmt.close();
			}catch(Exception e){}
		}
		if( conn != null ){
			try{
				conn.close();
			}catch(Exception e){}
		}
	}
	
	public void closeConnection(){
		closeConnection(conn);
	}
	
	public static void closeConnection(Connection conn){
		closeConnections(conn,null,null);
	}
	public static void closeStatement(Statement stmt){
		closeConnections(null,stmt,null);
	}
	public static void closeResultSet(ResultSet rs){
		closeConnections(null,null,rs);
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////
	/*public static Connection getConnection(String theServer, String thePort, String theDatabase) 
								throws SQLException {
		try {
                Class.forName(driverName);
        } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
        }
		String connStr;
		String server = "hivedev1";
		String port = "10000";
		String database = "default";
		
		// Check parameters
		if(theServer != ""){
			server = theServer;
		}
		if(thePort != ""){
			port = thePort;
		}
		if(theDatabase != "") {
			database = theDatabase;
		}
		
		connStr = "jdbc:hive://" + server + ":" + port + "/" + database;
		return DriverManager.getConnection(connStr);		
	}
	
	public static void _closeConnection(Connection conn) throws SQLException{
		conn.close();
	}
	
	// load data into a hive table
	public boolean loadData(Connection conn, String fileName, String tableName) throws SQLException {
		boolean local = true;
		boolean overwrite = false;
		loadData(conn, fileName, tableName, local, overwrite);
		return true;
	}
	
	public boolean loadData(Connection conn, String fileName, String tableName, boolean local, boolean overwrite) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "LOAD DATA ";
	    
	    if(local == true){
	    	sql = sql + "LOCAL ";
	    }
	    sql = sql + "INPATH '" + fileName + "' ";
	    
	    if(overwrite == true) {
	    	sql = sql + "OVERWRITE ";
	    }
	    sql = sql + "INTO TABLE " + tableName + ";";
	    
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	} 
	
	// the database operations
	public boolean useDatabase(Connection conn, String database) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "use " + database;
	    System.out.print("Running: " + sql);
	    stmt.executeQuery(sql);
	    stmt.close();
	    return true;
	}
	
	public boolean showTables(Connection conn) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "show tables;";
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}
	
	// the table DDL operations
	public boolean createTable(Connection conn, String theSql) throws SQLException {
		    Statement stmt = conn.createStatement();
		    String sql = theSql;
		    System.out.print("Running: " + sql);
		    stmt.executeUpdate(sql);
		    stmt.close();
		    return true;
	}
	
	public boolean dropTable(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "drop table " + tableName;
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}

	public boolean describeTable(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    String sql = "describe " + tableName;
	    System.out.print("Running: " + sql);
	    stmt.executeUpdate(sql);
	    stmt.close();
	    return true;
	}
	
	// the table DML operations
	public ResultSet executeSql(Connection conn, String theSql) throws SQLException {
	    Statement stmt = conn.createStatement();
	    ResultSet res;
	    String sql = theSql;
	    System.out.print("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    stmt.close();
	    return res;
	}
	public ResultSet selectAllSql(Connection conn, String tableName) throws SQLException {
		    Statement stmt = conn.createStatement();
		    ResultSet res;
		    String sql = "select * from " + tableName;
		    System.out.print("Running: " + sql);
		    res = stmt.executeQuery(sql);
		    stmt.close();
		    return res;
	}
	
	public ResultSet selectCountSql(Connection conn, String tableName) throws SQLException {
	    Statement stmt = conn.createStatement();
	    ResultSet res;
	    String sql = "select count(*) from " + tableName;
	    System.out.print("Running: " + sql);
	    res = stmt.executeQuery(sql);
	    stmt.close();
	    return res;
	}
	
	public void printResultSet(ResultSet res) throws SQLException{
		printResultSet(res, "data");
	}
	
	public void printResultSet(ResultSet res, String col) throws SQLException{
		while (res.next()) {
			System.out.println(res.getString(col));
			System.out.print(", ");
		}
	}*/
}
