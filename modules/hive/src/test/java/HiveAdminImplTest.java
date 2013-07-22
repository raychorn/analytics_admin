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

public class HiveAdminImplTest
{
   private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
   private static String serverName = "10.110.74.153";
   private static String databaseName = "test";
   private static final String tableName = "event_details";
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
         
         List<String> cols = new ArrayList<String>();
         cols.add("key");
         cols.add("value");
         List<String> types = new ArrayList<String>();
         types.add("int");
         types.add("string");
         dao.createTable(tableName,cols,types);
         assertTrue(true);
      }
      catch (HiveDAOException e)
      {  
         assertTrue(e.getMessage(), false);
      }
   }
   @After
   public void tearDown()
   {
      Statement stmt = null;
      try
      {
         stmt = dao.getConnectionManager().createStatement();
         stmt.execute("drop table " +tableName);
         stmt.execute("drop table " +tableName+"_new");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
        HiveConnectionManager.closeStatement(stmt);
      }
   }
   
   /**
    * sunny day scenario
    */
   @Test
   public void testCreateTable() throws HiveDAOException
   {
      List<String> cols = new ArrayList<String>();
      cols.add("col1");
      cols.add("col2");
      cols.add("col3");
      List<String> types = new ArrayList<String>();
      types.add("int");
      types.add("string");
      types.add("string");
      dao.createTable(tableName+"_new",cols,types);
      assertTrue(true);
   }
   
   /**
    * Check exception - non-existing table
    */
   @Test(expected=HiveDAOException.class)
   public void testCreateTableNegative1() throws HiveDAOException
   {
      List<String> cols = new ArrayList<String>();
      cols.add("key");
      cols.add("value");
      List<String> types = new ArrayList<String>();
      types.add("int");
      types.add("string");
      dao.createTable("*-*", cols,types);
   }
   
   /**
    * Check exception - number of number of column names does not equal the number of column types
    */
   @Test
   public void testCreateTableNegative2()
   {
      try
      {
         List<String> cols = new ArrayList<String>();
         cols.add("key");
         cols.add("value");
         List<String> types = new ArrayList<String>();
         types.add("int");
         types.add("string1");
         types.add("string2");
         dao.createTable(tableName, cols,types);
         assertTrue(false);
      }
      catch (HiveDAOException e)
      {  
         if(e.getMessage().contains("number of column names does not equal the number of column types"))
            assertTrue(true);
         else
            assertTrue(e.getMessage(), false);
      }
    }
   
   /**
    * Check Illegal argument exception 
    */
   @Test(expected=IllegalArgumentException.class)
   public void testCreateTableNegative3()
      throws HiveDAOException, IllegalArgumentException
   {
      List<String> cols = null;
      List<String> types = new ArrayList<String>();
      types.add("int");
      types.add("string1");
      types.add("string2");
      dao.createTable(tableName, cols,types);
   }

    /**
    * sunny day scenario
    */
   @Test
   public void testCreateTableWithPartitions() throws HiveDAOException
   {
      List<String> colNames = new ArrayList<String>();
      colNames.add("uuid"); colNames.add("device_type"); colNames.add("user_count");
      List<String> types = new ArrayList<String>();
      types.add("string"); types.add("string"); types.add("int");

      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "STRING");
      colNameValues.put("last_device_date", "STRING");
      dao.createTable(tableName+"_partitions",colNames,types, colNameValues);
      assertTrue(true);
   }

   /**
    * No. of cols != No. of Values
    */
   @Test
   public void testCreateTableWithPartitionsNegative1() throws HiveDAOException
   {
      try
      {
        List<String> colNames = new ArrayList<String>();
        colNames.add("uuid");colNames.add("device_type");
        List<String> types = new ArrayList<String>();
        types.add("string");
        types.add("string");
        types.add("int");
        Map colNameValues = new HashMap();
        colNameValues.put("device_model", "STRING");
        colNameValues.put("last_device_date", "STRING");
        dao.createTable(tableName+"_partitions",colNames,types, colNameValues);
        assertTrue(true);
      }
      catch (HiveDAOException e)
      {
         if(e.getMessage().contains("number of column names does not equal the number of column types"))
            assertTrue(true);
         else
            assertTrue(e.getMessage(), false);
      }

   }
    /**
    * No. of cols and No. of Values are null
    */
   @Test(expected=IllegalArgumentException.class)
   public void testCreateTableWithPartitionsNegative2() throws HiveDAOException, IllegalArgumentException
   {
        List<String> colNames = new ArrayList<String>();
        colNames = null;
        List<String> types = new ArrayList<String>();
        types = null;
        Map colNameValues = new HashMap();
        colNameValues.put("device_model", "STRING");
        colNameValues.put("last_device_date", "STRING");
        dao.createTable(tableName+"_partitions",colNames,types, colNameValues);
   }
    /**
    * NameValue map is null
    */
    @Test(expected=IllegalArgumentException.class)
   public void testCreateTableWithPartitionsNegative3() throws HiveDAOException, IllegalArgumentException
   {
        List<String> colNames = new ArrayList<String>();
        colNames.add("uuid"); colNames.add("device_type");
        colNames.add("user_count");
        List<String> types = new ArrayList<String>();
        types.add("string");
        types.add("string");
        types.add("int");
        Map colNameValues = new HashMap();
        colNameValues = null;
        dao.createTable(tableName+"_partitions",colNames,types, colNameValues);
        assertTrue(true);
   }

    /**
    * sunny day scenario
    */
   @Test
   public void testAddPartitionToTable() throws HiveDAOException
   {
      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "U598");
      colNameValues.put("last_device_date","2011-07-14" );
      dao.addPartitionToTable(tableName+"_partitions", colNameValues);
      assertTrue(true);
   }
    /**
    * No partition values provided
    */
   @Test(expected=IllegalArgumentException.class)
   public void testAddPartitionToTableNegative1() throws HiveDAOException, IllegalArgumentException
   {
     Map colNameValues = new HashMap();
     colNameValues = null;
     dao.addPartitionToTable(tableName+"_partitions", colNameValues);
     assertTrue(true);
   }
   /**
    * Add Partitions to an un-partitioned table;
    */
   @Test(expected=HiveDAOException.class)
   public void testAddPartitionToTableNegative2() throws HiveDAOException
   {
      Map colNameValues = new HashMap();
      colNameValues.put("device_model", "U598");
      colNameValues.put("last_device_date","2011-07-14" );
      dao.addPartitionToTable(tableName, colNameValues);
   }
    /**
    * Empty values provided to partitions.
    */
   @Test(expected=HiveDAOException.class)
   public void testAddPartitionToTableNegative3() throws HiveDAOException
   {
      Map colNameValues = new HashMap();
      colNameValues.put("", "");
      colNameValues.put("","" );
      dao.addPartitionToTable(tableName, colNameValues);
   }
   /**
    * sunny day scenario
    */
   @Test
   public void testDescribeTable() throws HiveDAOException
   {
      dao.describeTable(tableName);
      assertTrue(true);
   }
   
   /**
    * Check exception - non-existing table
    */
   @Test(expected=HiveDAOException.class)
   public void testDescribeTableNegative1() throws HiveDAOException
   {
      dao.describeTable(tableName+"_invalid_name");
   }
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testDescribeTableNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      dao.describeTable(null);
   }
   
   
   /**
    * sunny day scenario
    */
   @Test
   public void testShowTables() throws HiveDAOException
   {
      String actual = dao.showTables();
      assertTrue(actual.contains(tableName.toLowerCase()));
   }
   
   /**
    * sunny day scenario
    */
   @Test
   public void testSelectAllRows() throws HiveDAOException 
   {
      String expected = "{\"count\":0,\"data\":[],\"columns\":[\"key\",\"value\"]}";
      String actual = dao.selectAllRows(tableName);
      assertTrue(actual.equalsIgnoreCase(expected));
   }
   
   /**
    * Check exception - non-existing table
    */
   @Test(expected = HiveDAOException.class)
   public void testSelectAllRowsNegative1() throws HiveDAOException
   {
      dao.selectAllRows(tableName + "_non_existent");
   }
   
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testSelectAllRowsNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      dao.selectAllRows(null);
   }
   
   /**
    * sunny day scenario
    */
   @Test
   public void testGetNumberOfRows() throws HiveDAOException
   {
      String expected = "1";
      String actual = dao.getNumberOfRows(tableName);
      assertTrue(actual.equalsIgnoreCase(expected));
   }
   
   
   /**
    * Check exception - non-existing table
    */
   @Test(expected=HiveDAOException.class)
   public void testGetNumberOfRowsNegative1() throws HiveDAOException
   {
      dao.getNumberOfRows(tableName +"_non_existent");
   }
   
   @Test(expected=IllegalArgumentException.class)
   /**
    * Check Illegal argument exception
    */
   public void testGetNumberOfRowsNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      dao.getNumberOfRows(null);
   }
   
   /**
   * Sunny day scenario
   */
  @Test
  public void testDropPartitionFromTable() throws HiveDAOException
  {
     Map colNameValues = new HashMap();
     colNameValues.put("device_model", "U598");
     colNameValues.put("last_device_date","2011-07-14" );
     dao.dropPartitionFromTable(tableName + "_partitions", colNameValues);
     assertTrue(true);
  }
   /**
   * No partitions are provided.
   */
  @Test(expected=IllegalArgumentException.class)
  public void testDropPartitionFromTableNegative1() throws HiveDAOException, IllegalArgumentException
  {
     Map colNameValues = new HashMap();
     colNameValues = null;
     dao.dropPartitionFromTable(tableName + "_partitions", colNameValues);
     assertTrue(true);
  }
    /**
    * Empty values provided to partitions.
    */
  @Test(expected=HiveDAOException.class)
  public void testDropPartitionFromTableNegative2() throws HiveDAOException
  {
     Map colNameValues = new HashMap();
     colNameValues.put("", "");
     colNameValues.put("", "" );
     dao.dropPartitionFromTable(tableName + "_partitions", colNameValues);
  }
  /**
   * Sunny day scenario
   */
  @Test
  public void testDropTable() throws HiveDAOException
  {
     dao.dropTable(tableName);
     assertTrue(true);
  }
  /**
   * Sunny day scenario
   */
  @Test
  public void testDropPartitionedTable() throws HiveDAOException
  {
     dao.dropTable(tableName+"_partitions");
     assertTrue(true);
  }
   
   /**
    * Check exception - non-existing table
    */
   @Test(expected=HiveDAOException.class)
   public void testDropTableNegative1() throws HiveDAOException
   {
      dao.dropTable("---*---");
   }
   
   
   /**
    * Check Illegal argument exception
    */
   @Test(expected=IllegalArgumentException.class)
   public void testDropTableNegative2()
      throws HiveDAOException,IllegalArgumentException
   {
      dao.dropTable(null);
   }
}
