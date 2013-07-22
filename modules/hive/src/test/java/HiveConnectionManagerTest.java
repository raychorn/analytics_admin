import java.sql.SQLException;
import java.sql.Statement;

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

public class HiveConnectionManagerTest
{
   private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
   private static String server = "10.110.74.153";
   private static String dbName = "test";
   private static final String tableName = "event_details";
   private static final String port = "10000";

   
   @Test
   public void testConstructorHiveConnectionManager1()
      throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      assertTrue(hcm.getServerAddress().equalsIgnoreCase(server));
   }
   @Test
   public void testConstructorHiveConnectionManager2()
      throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      assertTrue(hcm.getPort().equalsIgnoreCase(port));
   }
   @Test
   public void testConstructorHiveConnectionManager3()
      throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      assertTrue(hcm.getDatabaseName().equalsIgnoreCase(dbName));
   }
   @Test
   public void testConstructorHiveConnectionManager4()
      throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      assertTrue(hcm.getConnection() != null);
   }
   
   @Test(expected=HiveDAOException.class)
   public void testConstructorHiveConnectionManagerNegative1()
      throws HiveDAOException
   {
      String dbName = "nonexistent";
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
   }
   @Test(expected=IllegalArgumentException.class)
   public void testConstructorHiveConnectionManagerNegative2()
      throws HiveDAOException, IllegalArgumentException
   {
      String server = null;
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
   }
   @Test
   public void testCreateConnection() 
      throws SQLException,HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      hcm.getConnection().close();
      hcm.createConnection(server,port,dbName);
      assertTrue(!hcm.getConnection().isClosed());
   }
   @Test
   /**
    * Check HiveDAOException exception
    */
   public void testCreateConnectionNegative1() 
   {
      HiveConnectionManager hcm = null;
      
      try
      {
         hcm = new HiveConnectionManager(server,port,dbName);
      }
      catch (HiveDAOException e)
      {
         assertTrue(e.getMessage(),false);
      }
      try
      {
         String port = "0000";
         hcm.createConnection(server,port,dbName);
      }
      catch (HiveDAOException e)
      {
         assertTrue(e.getMessage().contains("Unable to establish connection"));
      }
   }
   @Test
   public void testCloseConnection() 
      throws SQLException,HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      hcm.closeConnection();
      assertTrue(hcm.getConnection().isClosed());
   }
   @Test
   public void testCreateStatement() throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      Statement statement = hcm.createStatement();
      assertTrue(statement != null);
   }
   @Test(expected=HiveDAOException.class)
   public void testCreateStatementNegative1()
      throws SQLException,HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      hcm.getConnection().close();
      Statement statement = hcm.createStatement();
   }
   
   @Test
   public void testOpenDatabase() throws HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      hcm.openDatabase();
   }
   @Test(expected=HiveDAOException.class)
   public void testOpenDatabaseNegative1()
      throws SQLException,HiveDAOException
   {
      HiveConnectionManager hcm = new HiveConnectionManager(server,port,dbName);
      hcm.getConnection().close();
      hcm.openDatabase();
   }
}
