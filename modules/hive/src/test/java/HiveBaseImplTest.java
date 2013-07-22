import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.smithmicro.analytics.hive.server.HiveAdminDAO;
import org.smithmicro.analytics.hive.server.HiveAdminImpl;
import org.smithmicro.analytics.hive.server.HiveBaseImpl;
import org.smithmicro.analytics.hive.server.HiveConnectionManager;
import org.smithmicro.analytics.hive.server.HiveDAO;
import org.smithmicro.analytics.hive.server.HiveDAOException;
import org.smithmicro.analytics.hive.server.HiveDAOFactory;
import org.json.simple.JSONObject;

public class HiveBaseImplTest
{
   private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
   private static String serverName = "10.110.74.153";
   private static String databaseName = "test";
   private static String serverNameMsql = "10.110.74.153";
   private static final String tableName = "event_details";
   private static final String tableNamePartitions = "event_details_partitions";
   private static final String port = "10000";
   private static HiveAdminDAO dao;
   @BeforeClass
   public static void oneTimeSetUp()
   {
      try
      {
         dao =(HiveAdminDAO)HiveDAOFactory.getHiveDAO(serverName,
               port, databaseName, HiveDAOFactory.HiveDAOType.ADMIN);
      }
      catch (HiveDAOException e)
      {
         e.printStackTrace();
      }   
   }
   
   @AfterClass
   public static void oneTimeTearDown()
   {
      dao.closeConnection();
   }
   
   @Before
   public void setUp()
   {
      try
      {
          List<String> colNames = new ArrayList<String>();
          colNames.add("uuid"); colNames.add("device_type"); colNames.add("user_count");
          List<String> colTypes = new ArrayList<String>();
          colTypes.add("string"); colTypes.add("string"); colTypes.add("int");

          List<String> cols = new ArrayList<String>();
          cols.add("key");     cols.add("value");
          List<String> types = new ArrayList<String>();
          types.add("int");    types.add("string");
          dao.createTable(tableName, cols,types);

          Map colNamesTypes = new HashMap();
          colNamesTypes.put("device_model", "STRING");
          colNamesTypes.put("last_device_date", "STRING");
          dao.createTable(tableNamePartitions,colNames,colTypes, colNamesTypes);
      }
      catch (HiveDAOException e)
      {
         e.printStackTrace();
      }
   }
   @After
   public void tearDown()
   {
      try
      {
         dao.dropTable(tableName);
         dao.dropTable(tableName + "_new");
         dao.dropTable(tableNamePartitions);
      }
      catch (HiveDAOException e)
      {
         e.printStackTrace();
      }
   }
   @Test 
   public void testLoadData1() throws HiveDAOException
   {
      String expected = "{\"count\":3,\"data\":[[1,\"foo\"],[2,\"bar\"],[3,\"fizz\"]]," +
      		"\"columns\":[\"key\",\"value\"]}";
       dao.loadData("/home/hadoop/test/test.txt", tableName);
      JSONObject actual = 
         ((HiveAdminImpl) dao).executeQuery("select * from " +tableName);
      assertTrue(actual.toString().equals(expected));
   }
   
   @Test
   /**
    * Overwrite false
    */
   public void testLoadData2() throws HiveDAOException
   {
      String expected = "{\"count\":6,\"data\":[[1,\"foo\"],[2,\"bar\"],[3,\"fizz\"]," +
      		"[1,\"foo\"],[2,\"bar\"],[3,\"fizz\"]],\"columns\":[\"key\",\"value\"]}";
      dao.loadData("/home/hadoop/test.txt", tableName, true, false);
      dao.loadData("/home/hadoop/test.txt", tableName, true, false);
      JSONObject actual = 
         ((HiveAdminImpl) dao).executeQuery("select * from " +tableName);
      assertTrue(actual.toString().equals(expected));
   }
   
   
   /**
    * Omit using local
    */
   @Test(expected=HiveDAOException.class)
   public void testLoadData3() throws HiveDAOException
   {
      dao.loadData("/home/hadoop/test/test.txt", tableName, false, true);
   }
   
   
   /**
    * check HiveDAOException exception
    */
   @Test
   public void testLoadDataNegative1()
   {
      try
      {
         dao.loadData("/home/hadoop/non-existent.txt", tableName, true, false);
         assertTrue(false);
      }
      catch (HiveDAOException e)
      {
         assertTrue(e.getMessage().contains("unable to execute: LOAD DATA"));
      }
   }
   
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testLoadDataNegative2()
      throws HiveDAOException,IllegalArgumentException 
   {
      String fileName = null;
      dao.loadData(fileName, tableName, true, false);
   }
   
   @Test
   public void testExecuteQuery() throws HiveDAOException
   {
      String expected = "{\"count\":2,\"data\":[[\"key\",\"int\",\"\"]," +
      		"[\"value\",\"string\",\"\"]],\"columns\":[\"col_name\"," +
      		"\"data_type\",\"comment\"]}";
      JSONObject json = dao.executeQuery("describe " + tableName);
      assertTrue(json != null);
      assertTrue(json.toString().equals(expected));
   }
   
   
   /**
    * Check exception - invalid select
    */
   @Test(expected=HiveDAOException.class)
   public void testExecuteQueryNegative1() throws HiveDAOException
   {
      JSONObject json = dao.executeQuery("select from " + tableName);
   }
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testExecuteQueryNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      JSONObject json = dao.executeQuery(null);
   }
   
//   @Test
//   public void testSetConnectionManager() throws HiveDAOException
//   {
//      boolean status = false;
//      HiveDAO dao = new HiveBaseImpl();
//      status = dao.setConnectionManager(new HiveConnectionManager(serverName,
//            port, databaseName));
//      assertTrue(status);
//   }
//
//   @Test
//   public void testSetConnectionManagerNegative1()
//   {
//      boolean status = false;
//      HiveDAO dao = new HiveBaseImpl();
//      status = dao.setConnectionManager(null);
//      assertTrue(!status);
//   }
   
   @Test
   public void testInsertQueryOutputToHiveTable() throws HiveDAOException
   {
      boolean status = false;
      String query = "select * from " + tableName;
      status = dao.insertQueryOutputToHiveTable(query, tableName);
      assertTrue(status);
   }
   
   @Test
   /**
    * check HiveDAOException exception - invalid sql syntax
    */
   public void testInsertQueryOutputToHiveTableNegative1()
   {
      boolean status = false;
      String query = "select from " + tableName;
      try
      {
         status = dao.insertQueryOutputToHiveTable(query, tableName);
         assertTrue(false);
      }
      catch (HiveDAOException e)
      {
         assertTrue(e.getMessage().contains("unable to execute: INSERT " +
         		"OVERWRITE TABLE"));
      }
   }
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testInsertQueryOutputToHiveTableNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      dao.insertQueryOutputToHiveTable(null, tableName);
   }

@Test
   public void testInsertQueryOutputToHiveTableWithPartitions() throws HiveDAOException
   {
      boolean status = false;
      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "U598");
      colNameValues.put("last_device_date","2011-07-14" );
      String query = "SELECT uuid, device_type, count(uuid) FROM login_summary WHERE device_model='ATHENA' and last_device_date='2011-07-13' GROUP BY uuid, device_type";
      status = dao.insertQueryOutputToHiveTable(query, tableNamePartitions, colNameValues);
      assertTrue(status);
   }

   /**
    * check HiveDAOException exception - invalid sql syntax
    */
   @Test(expected=HiveDAOException.class)
   public void testInsertQueryOutputToHiveTableWithPartitionsNegative1() throws HiveDAOException
   {
      boolean status = false;
      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "U598");
      colNameValues.put("last_device_date","2011-07-14" );
      String query = "select from " + tableName;
      status = dao.insertQueryOutputToHiveTable(query, tableNamePartitions, colNameValues);
   }

   /**
    * Check Illegal argument exception. No query provided
    */
   @Test(expected=IllegalArgumentException.class)
   public void testInsertQueryOutputToHiveTableWithPartitionsNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "U598");
      colNameValues.put("last_device_date","2011-07-14" );
      dao.insertQueryOutputToHiveTable(null, tableNamePartitions, colNameValues);
   }
    /**
        * Check Illegal argument exception. No query provided
        */
       @Test(expected=IllegalArgumentException.class)
       public void testInsertQueryOutputToHiveTableWithPartitionsNegative3()
          throws HiveDAOException,IllegalArgumentException
       {
          Map colNameValues = null;
          dao.insertQueryOutputToHiveTable(null, tableNamePartitions, colNameValues);
       }

   @Test
   public void testExportQueryOutputToMySQLTable() throws HiveDAOException
   {
      boolean status = false;
      String[] colsArr = {"`key`","value"};
      status = dao.exportQueryOutputToMySQLTable(serverNameMsql,"3306",
           "analytics_admin_dev","analytics_admin","gyd82jd",tableName,colsArr);
      assertTrue(status);
   }
   
   /**
    * Check HiveDAOException exception
    */
   @Test(expected=HiveDAOException.class)
   public void testExportQueryOutputToMySQLTableNegative1()
      throws HiveDAOException
   {
      String tableName = "non-existent";
      String[] colsArr = {"`key`","value"};
      dao.exportQueryOutputToMySQLTable(serverNameMsql,"3306",
           "analytics_admin_dev","analytics_admin","gyd82jd",tableName,colsArr);
   }  
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testExportQueryOutputToMySQLTableNegative2()
      throws HiveDAOException,IllegalArgumentException 
   {
      String serverNameMsql = null;
      String[] colsArr = {"`key`","value"};
      dao.exportQueryOutputToMySQLTable(serverNameMsql,"3306",
            "analytics_admin_dev","analytics_admin",
            "gyd82jd","testhivetable",colsArr);
   }
   
   @Test
   public void testResultSetToJson() 
      throws HiveDAOException,SQLException
   {
      Statement stmt = null;
      ResultSet rs = null;
      JSONObject jObject = null;
      StringBuffer sql = null;
      String expected = "{\"count\":0,\"data\":[],\"columns\":[\"_c0\"]}";
      
      stmt = dao.getConnectionManager().createStatement();
      rs = null;
      jObject = null;
         
      sql = new StringBuffer("select 1 from " + tableName);
      rs = stmt.executeQuery(sql.toString());
      
      jObject = HiveBaseImpl.resultSetToJson(rs);
      assertTrue(jObject.toString().equals(expected));      
   }
   
   @Test(expected=IllegalArgumentException.class)
   public void testResultSetToJsonNegative1()
      throws Exception,IllegalArgumentException
   {
      HiveBaseImpl.resultSetToJson(null);
   }
   
    
   @Test
   public void testStoreQueryOutputToHiveTable() throws HiveDAOException
   {
      boolean status = false;
      String query = "select * from " + tableName;
      status = dao.storeQueryOutputToHiveTable(query, tableName + "_new");
      assertTrue(status);
   }
   
   /**
    * Check HiveDAOException exception
    */
   @Test(expected=HiveDAOException.class)
   public void testStoreQueryOutputToHiveTableNegative1()
       throws HiveDAOException
   {
      String query = "invalid select from " + tableName;
      dao.storeQueryOutputToHiveTable(query, tableName + "_new");
   }
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testStoreQueryOutputToHiveTableNegative2()
      throws HiveDAOException,IllegalArgumentException 
   {
      String query = "select * from " + tableName;
      dao.storeQueryOutputToHiveTable(query, null);
   }
   
}