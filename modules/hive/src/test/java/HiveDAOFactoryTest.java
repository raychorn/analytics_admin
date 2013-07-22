import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

public class HiveDAOFactoryTest
{
   private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
   private static String server = "10.110.74.153";
   private static String dbName = "test";
   private static final String tableName = "event_details";
   private static final String port = "10000";
   
   @Test
   public void testGetHiveDAO1() throws HiveDAOException
   {
     HiveDAO dao = HiveDAOFactory.getHiveDAO(server,
            port, dbName, HiveDAOFactory.HiveDAOType.ADMIN);
     assertTrue(dao instanceof HiveAdminImpl);
   }
   @Test
   public void testGetHiveDAO2() throws HiveDAOException
   {
     HiveDAO dao = HiveDAOFactory.getHiveDAO(server,
            port, dbName, HiveDAOFactory.HiveDAOType.BASE);
     assertTrue(dao instanceof HiveBaseImpl);
   }
   @Test(expected=HiveDAOException.class)
   public void testGetHiveDAONegative1() throws HiveDAOException
   {
     String port = "0000";
     HiveDAO dao = HiveDAOFactory.getHiveDAO(server,
            port, dbName, HiveDAOFactory.HiveDAOType.BASE);
   }
   
   @Test(expected=IllegalArgumentException.class)
   public void testGetHiveDAONegative2() 
      throws HiveDAOException, IllegalArgumentException
   {
     String server = null;
     HiveDAO dao = HiveDAOFactory.getHiveDAO(server,
            port, dbName, HiveDAOFactory.HiveDAOType.BASE);
   }
}
