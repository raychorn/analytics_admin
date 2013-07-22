/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */

package org.smithmicro.analytics.hive.server;


import java.lang.reflect.MalformedParameterizedTypeException;
import java.sql.Connection;
import java.util.*;

import org.apache.hadoop.util.hash.Hash;
import org.json.simple.JSONObject;

import com.google.gson.JsonObject;

import javax.management.Query;
import javax.print.DocFlavor;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String serverName = "10.110.74.153";
	private static String databaseName = "test";
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	    // Check how many arguments were passed in
		if(args.length > 0)
	    {
			serverName = args[0];
			try {
				databaseName = args[1];
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("Using \"default\" database.");
			}
	    }
	    System.out.println("Starting JDBC Client...");
		newAPITestCode();
		//oldTestCode();
	    //testStoreQueryOutputToHiveTable("select * from testHiveDriverTable", "table_dummy_4");
	    //testExecuteQuery("select * from testHiveDriverTable");
        //testExecuteQuery("describe testHiveDriverTable");
		//testAddJar("/home/hadoop/analytics/hive/lib/mysql-connector-java-5.1.16-bin.jar");
        //testAddUDF("/home/hadoop/analytics/hive/lib/hive_contrib.jar","org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput","dboutput");
        //testExecUDF("/home/hadoop/analytics/hive/lib/DayOfWeek.jar","com.smithmicro.hive.udf.DayOfWeek","DayOfWeek","select DayOfWeek('2011-05-10') from testHiveDriverTable");
        //testExportMysql();
		//testExecuteQueryAndStoreQueryOutputToHiveTable(5);
		
		
	}
	public static void newAPITestCode() {
		HiveAdminDAO dao = null;
		String tableName = "testHiveDriverTable";
		try{
			System.out.println("connecting to "+serverName+ ", using database "+databaseName );
			dao = (HiveAdminDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.ADMIN);

			System.out.println("dropping table "+tableName);
			dao.dropTable(tableName);
			System.out.println("dropped: "+tableName+"\ncreating table "+tableName+" (key int, value string)");
			List<String> cols = new ArrayList<String>();
			cols.add("key");
			cols.add("value");
			List<String> types = new ArrayList<String>();
			types.add("int");
			types.add("string");
			dao.createTable(tableName, cols,types);
			System.out.println("created table "+tableName+"\nloading data into table "+tableName);
			dao.loadData("/home/hadoop/analytics/library/sample_data/a.txt", tableName);
			System.out.println("loaded data");

            String actual = dao.getNumberOfRows(tableName);
            // Changes made by KK
            String tableName1 = "event_details_partitions";
            List<String> colNames = new ArrayList<String>();
            colNames.add("uuid"); colNames.add("device_type");
            colNames.add("user_count");

            List<String> colTypes = new ArrayList<String>();
            colTypes.add("STRING"); colTypes.add("STRING");colTypes.add("INT");

            System.out.println("created table "+tableName1+"\n Insert Query output into table "+tableName1);
            Map colNameValues = new HashMap();
            colNameValues.put("device_model", "U598");
            colNameValues.put("last_device_date","2011-07-14" );

            System.out.println("add partition values to already partitioned table " + tableName1);
            dao.addPartitionToTable(tableName1, colNameValues);

            String query = "SELECT uuid, device_type, count(uuid) FROM login_summary WHERE device_model='ATHENA' and last_device_date='2011-07-13' GROUP BY uuid, device_type";
            dao.insertQueryOutputToHiveTable(query, tableName1, colNameValues);

            System.out.println("drop partition values from table " + tableName1);
            dao.dropPartitionFromTable(tableName1, colNameValues);
           // dao.insertQueryOutputToHiveTable(query, "test");

            System.out.println("show tables: "+dao.showTables());
			System.out.println("describe "+tableName+": "+dao.describeTable(tableName));
			System.out.println("select * from testHiveDriverTable: "+tableName+": "+dao.selectAllRows(tableName));
			System.out.println("select count(*) from testHiveDriverTable: "+tableName+": "+dao.getNumberOfRows(tableName));


		}catch(HiveDAOException e){
			System.out.println(e.getMessage());
			e.getCause().printStackTrace();
		}finally
		{
			dao.closeConnection();
		}
		
	}

   public static void testExecuteQueryAndStoreQueryOutputToHiveTable(int nLoops) throws HiveDAOException
   {
      for (int i = 0 ; i < nLoops; i ++)
      {
         Random random = new Random();
         int randomInt = random.nextInt(1000);
         boolean status = testStoreQueryOutputToHiveTable("select * from testHiveDriverTable", "table_" +randomInt+ "_" +i);
        if (status)
            testExecuteQuery("select * from table_" +randomInt+ "_" +i);
      }
   }


   public static void testExecuteQuery(String query){
      
      try
      {
         HiveDAO dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
         System.out.println("Executing query: " + query);
         JSONObject json = dao.executeQuery(query);
         System.out.println("Return json: " +json);
      }
      catch (HiveDAOException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
   }

   public static void testAddJar(String jar){

      try
      {
         HiveDAO dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
         System.out.println("Executing addJar: " + jar);
         Connection conn = dao.getConnectionManager().addJar(jar);
      }
      catch (HiveDAOException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }

   public static void testAddUDF(String jarPath, String className, String functionName){

      try
      {
         HiveDAO dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
         System.out.println("Executing addUDF: " + jarPath + " class: " + className + " function: " + functionName);
         Connection conn = dao.getConnectionManager().addUDF(jarPath,className,functionName);
      }
      catch (HiveDAOException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }

   public static void testExecUDF(String jarPath, String className, String functionName, String query){

      try
      {
         HiveDAO dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
         System.out.println("Executing addUDF: " + jarPath + " class: " + className + " function: " + functionName + " query: " + query);
         Connection conn = dao.getConnectionManager().addUDF(jarPath,className,functionName);
         JSONObject json = dao.executeQuery(query);
         System.out.println("Return json: " +json);
      }
      catch (HiveDAOException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }

   public static void testExportMysql() {
       String[] colsArr = {"`key`","value"};
       System.out.println(java.util.Arrays.toString(colsArr));
       StringBuffer cols = new StringBuffer();
       cols.append(org.apache.commons.lang.StringUtils.join(colsArr,','));
       System.out.println(cols.toString());
       StringBuffer detailCols = new StringBuffer();
       String[] detailArr = new String[colsArr.length];
       StringBuffer valuesCols = new StringBuffer();
       String[] valuesArr = new String[colsArr.length];
       //for (String s: colsArr)
       for (int i = 0; i < colsArr.length; i++)
       {
          detailArr[i] = "details."+colsArr[i];
          valuesArr[i] = "?";
          //detailCols.append("details.");
          //detailCols.append(s);
          //if (colsArr.length == 1) {
          //    cols.append(",");
          //}
       }
       detailCols.append(org.apache.commons.lang.StringUtils.join(detailArr,','));
       valuesCols.append(org.apache.commons.lang.StringUtils.join(valuesArr,','));
       System.out.println(detailCols.toString());
       System.out.println(valuesCols.toString());
       try
       {
         HiveDAO dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
         dao.exportQueryOutputToMySQLTable("hivedev1","3306","analytics_admin_dev","analytics_admin","gyd82jd","results_20",colsArr);
       }
        catch (HiveDAOException e)
       {
         // TODO Auto-generated catch block
         e.printStackTrace();
       }
   }
   
     public static boolean testStoreQueryOutputToHiveTable(String query, String tablename) throws HiveDAOException{
         
        HiveDAO dao= null;
        boolean status = false;
         try
         {
            dao = (HiveDAO)HiveDAOFactory.getHiveDAO(serverName, "10000", databaseName, HiveDAOFactory.HiveDAOType.BASE);
            status = dao.storeQueryOutputToHiveTable(query, tablename);
           }
         catch (HiveDAOException e)
         {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
         System.out.println("Is testStoreQueryOutputToHiveTable successfull:" +status);
         return status;
         
      }
	
/*	public static void oldTestCode() throws Exception{
		// Get HiveConnectionManager
		//HiveConnectionManager hiveManager = new HiveConnectionManager(serverName,"10000",databaseName);
		
		String s = "select * from testHiveDriverTable";
		//Connection con = DriverManager.getConnection("jdbc:hive://hivedev1:10000/" + databaseName, "", "");
		Connection con = HiveConnectionManager.getConnection(serverName, "10000", databaseName);
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		//stmt.executeQuery("use bakrie");
		//hiveManager.useDatabase(con, "bakrie"); //unnecessary 
		// show database tables
                System.out.println("Tables in Database: '" + databaseName + "'");
                ResultSet resDb = stmt.executeQuery("show tables");
                while (resDb.next()) {
                        System.out.println(resDb.getString(1));
			// Describe Tables
			String sql = "describe " + resDb.getString(1);
                	System.out.println("Running: " + sql);
                	//ResultSet res = stmt.executeQuery(sql);
                	//while (res.next()) {
                        //	System.out.println(res.getString(1) + "\t" + res.getString(2));
                	//}
                }
		stmt.executeQuery("drop table " + tableName);
		ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		System.out.println("rsToJson For Describe="+HiveBaseImpl.resultSetToJson(res));
		System.out.println("describe "+tableName);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
		
		// load data into table
		// NOTE: filepath has to be local to the hive server
		String filepath = "/home/hadoop/a.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		
		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		System.out.println("rsToJson="+HiveBaseImpl.resultSetToJson(res));
		/*while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}*/
		
		// regular hive query
	/*
    sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}*/
}
