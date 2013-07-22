    /*
    * Copyright (c) 2011. SmithMicro Software, Inc.
    * All Rights Reserved.
    */
    package org.smithmicro.analytics.hive.server;

    import java.lang.reflect.Array;
    import java.sql.ResultSet;
    import java.sql.ResultSetMetaData;
    import java.sql.SQLException;
    import java.sql.Statement;
    import java.util.ArrayList;
    import java.util.Collection;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.List;
    import java.util.Iterator;

    import org.apache.log4j.Logger;
    import org.json.simple.JSONArray;
    import org.json.simple.JSONObject;



    /**
    *
    * @author nstanford
    *
    */
    public class HiveBaseImpl implements HiveDAO {

    private static Logger logger = Logger.getLogger(HiveBaseImpl.class);
    private final static String NODE_NAME_COLUMN = "columns";
    private final static String NODE_NAME_COUNT = "count";
    private final static String NODE_NAME_RESULT = "data";


    protected HiveConnectionManager connManager;

    @Override
    public void setConnectionManager(HiveConnectionManager connectionManager) {
        this.connManager = connectionManager;
    }

    @Override
    public HiveConnectionManager getConnectionManager() {
        return this.connManager;
    }

    @Override
    public void loadData(String fileName, String tableName, boolean local,boolean overwrite) throws HiveDAOException{
        if(fileName == null || tableName == null)
	      throw new IllegalArgumentException(String.format("Parameters can't " +
	            "be null: fileName=%s, tableName=%s", fileName,tableName));
        Statement stmt = connManager.createStatement();

        StringBuffer sql = new StringBuffer("LOAD DATA ");

        if(local == true){
            sql = sql.append(" LOCAL ");
        }
        sql.append(" INPATH '");
        sql.append(fileName);
        sql.append("'");
        if(overwrite == true) {
            sql.append(" OVERWRITE ");
        }
        sql.append(" INTO TABLE ");
        sql.append(tableName);

        logger.info("Running: " + sql);
        try {
            stmt.execute(sql.toString());
        } catch (SQLException e) {
            logger.error("unable to execute: " + sql,e);
            throw new HiveDAOException(e,"unable to execute: "+sql);
        }finally{
            HiveConnectionManager.closeStatement(stmt);
        }
    }

    @Override
    public void loadData(String fileName, String tableName) throws HiveDAOException {
        loadData(fileName, tableName, true, false);
    }

    /*   public static JsonObject resultSetToJson(ResultSet rs) throws HiveDAOException {
      JsonObject result = new JsonObject();
      Gson gson = new Gson();
      try {
         ResultSetMetaData rsMeta = rs.getMetaData();
         int numberOfColumns = rsMeta.getColumnCount();
         List<String> columnNames = new ArrayList<String>();
         for(int i=1; i<= numberOfColumns; i++){
            columnNames.add(rsMeta.getColumnName(i));
         }
         result.addProperty("columns", gson.toJson(columnNames));
         List<String> rowValues = new ArrayList<String>();
         int row = 1;
         while( rs.next() ){
            for(int i=1; i<= numberOfColumns; i++){
               Object columnValue = rs.getObject(i);
               rowValues.add(String.valueOf(columnValue));
            }
            result.addProperty("row"+row, gson.toJson(rowValues));
            rowValues.clear();
            row++;
         }
      }catch(Exception e){
         throw new HiveDAOException(e);
      }
      return result;
    }*/

    @SuppressWarnings("unchecked")
    public static JSONObject resultSetToJson(ResultSet rs) throws HiveDAOException
    {
       if(rs == null)
	      throw new IllegalArgumentException(String.format("Parameters can't " +
                 "be null: rs=%s", rs));

       JSONObject obj = new JSONObject();
       JSONArray columnNames = new JSONArray();
       JSONArray columnValues = new JSONArray();

        try
        {
            ResultSetMetaData rsMeta = rs.getMetaData();
            int numberOfColumns = rsMeta.getColumnCount();

            for(int i=1; i<= numberOfColumns; i++){
               columnNames.add(rsMeta.getColumnName(i));
            }

            JSONArray inner = null;
            int row = 0;
            while( rs.next() )
            {
               inner = new JSONArray();
               for(int i=1; i<= numberOfColumns; i++)
               {
                  Object columnValue = rs.getObject(i);
                    String tmp = String.valueOf(columnValue);
                    if (checkIsNumber(tmp))
                       inner.add(Integer.parseInt(tmp));
                    else
                        inner.add(tmp);
                }
               columnValues.add(inner);
               row++;
            }

        obj.put(NODE_NAME_COLUMN, columnNames);
        obj.put(NODE_NAME_COUNT, row);
            obj.put(NODE_NAME_RESULT,columnValues);

        }catch(Exception e){
            throw new HiveDAOException(e);
        }
        return obj;
    }

    private static boolean checkIsNumber(String toCheck)
    {
       try {
          Integer.parseInt(toCheck);
          return true;
          } catch (NumberFormatException numForEx) {
          return false;
          }
    }
    @Override
    public void executeJsonQueries(String jsonFormatedQueries) throws HiveDAOException {
        throw new HiveDAOException("execute has not been implemented...");
    }


    /**
     * Returns a map of hive columns and types
     * @param tableName
     * @return
     * @throws HiveDAOException
    */

    /**
    public boolean hiveTableColumnsAndTypes(String tableName) throws HiveDAOException {

      boolean status = false;
      Statement stmt = connManager.createStatement();
      StringBuffer sql = new StringBuffer(" ");
      sql.append(tableName);
      sql.append(" AS ");
      try
      {
        stmt.executeQuery(sql.toString());
        status = true;
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      return status;
      }
       */


    /**
     * Returns a map of hive columns and types
     * @param tableName
     * @return
     * @throws HiveDAOException
    */
      public Map hiveTableColumnsAndTypes(String tableName) throws HiveDAOException {
        Statement stmt = connManager.createStatement();
        String sql = "Select * from "+ tableName;
        ResultSet rs = null;
        JSONObject jsonResult = new JSONObject();
        try
        {
            rs = stmt.executeQuery(sql);
            jsonResult = resultSetToJson(rs);
        }catch(Exception e){
            throw new HiveDAOException(e,"unable to Select from table "+tableName);
        }finally{
            HiveConnectionManager.closeConnections(null, stmt, rs);
        }

        Collection list =  jsonResult.values();
        Map<ArrayList, ArrayList> map = new HashMap();

        ArrayList dataTypeList = new ArrayList();
        ArrayList columnsList = new ArrayList();
        for ( int i = 0; i < ((JSONArray)jsonResult.get(NODE_NAME_COLUMN)).size(); i++)
            columnsList.add(((JSONArray)jsonResult.get(NODE_NAME_COLUMN)).get(i));
        for ( int i = 0; i < ((JSONArray)jsonResult.get(NODE_NAME_RESULT)).size(); i++)
            dataTypeList.add(((JSONArray) jsonResult.get(NODE_NAME_RESULT)).get(i));

        map.put(columnsList, dataTypeList);
        return map;
    }

    /**
    * Executes query and stores the output to a new table
    * @param query
    * @param table name
    * @return true if success
    */
    public boolean storeQueryOutputToHiveTable(String query, String tableName) throws HiveDAOException {

      if(query == null || tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: query=%s, tableName=%s", query,tableName));

      boolean status = false;
      Statement stmt = connManager.createStatement();
      StringBuffer sql = new StringBuffer("CREATE TABLE ");
      sql.append(tableName);
      sql.append(" AS ");
      sql.append(query);
      try
      {
        stmt.executeQuery(sql.toString());
        status = true;
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      return status;
    }

    /**
     * Inserts the query results/output to an existing Hive Table
     * @param query
     * @param tableName
     * @return
     * @throws HiveDAOException
    */
    public boolean insertQueryOutputToHiveTable(String query, String tableName) throws HiveDAOException {

      if(query == null || tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: query=%s, tableName=%s", query,tableName));

      boolean status = false;
      Statement stmt = connManager.createStatement();
      StringBuffer sql = new StringBuffer("INSERT OVERWRITE TABLE ");
      sql.append(tableName);
      sql.append(" ");
      sql.append(query);
      try
      {
        stmt.executeQuery(sql.toString());
        status = true;
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      return status;
    }
     /**
     * Inserts the query results/output to an existing Hive Table when partitions were provided
     * @param query
     * @param tableName
     * @return
     * @throws HiveDAOException
    */
    public boolean insertQueryOutputToHiveTable(String query, String tableName, Map partitions) throws HiveDAOException {

      if(query == null || tableName == null || partitions == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: query=%s, tableName=%s", query,tableName));
      boolean status = false;
      Statement stmt = connManager.createStatement();
      StringBuffer sql = new StringBuffer("INSERT OVERWRITE TABLE ");
      sql.append(tableName);
      sql.append(" PARTITION (");
      String enableDynamicPartitionQuery = "set hive.exec.dynamic.partition=true";

      Iterator it = partitions.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry colNameValues = (Map.Entry)it.next();
        String partColName = colNameValues.getKey().toString();
        String partColType = colNameValues.getValue().toString();
        if ( !partColType.equals(""))
          sql.append(partColName + "='" + partColType + "'");
        else
          sql.append(partColName);
        if (it.hasNext())
            sql.append(", ");
        else
            sql.append(") ");
      }
      sql.append(query);
      try
      {
        logger.info("executing set dynamic partitions to true: " + enableDynamicPartitionQuery);
        stmt.executeQuery(enableDynamicPartitionQuery);
        logger.info("executing insert query output to hive table with partitions statement: " + sql.toString());
        stmt.executeQuery(sql.toString());
        status = true;
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      return status;
    }
    /**
     * Not yet implemented
     * @param tableName
     * @return
     * @throws HiveDAOException
    * #TODO: Use MySQL Connection Manager Jar and have Rails construct the database parameters
    */
    public boolean exportQueryOutputToMySQLTable(String server, String port, String database, String username, String password, String tableName, String[] colNames) throws HiveDAOException {

      if(server == null || port == null || database == null || username == null
            || password == null || tableName == null || colNames == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: server=%s, port=%s, database=%s, username =%s, " +
               "password=%s, tableName=%s,colNames=%s",
               server,port,database,username,password,tableName,colNames));
      //String[] colNames = {"`key`","value"};
      String[] detailCols = new String[colNames.length];
      String[] valuesCols = new String[colNames.length];
      for (int i = 0; i < colNames.length; i++)
      {
          detailCols[i] = "details."+colNames[i];
          valuesCols[i] = "?";
      }
      boolean status = false;
      Statement stmt = connManager.createStatement();
      StringBuffer sql = new StringBuffer("SELECT dboutput('jdbc:mysql://");
      sql.append(server);
      sql.append(":");
      sql.append(port);
      sql.append("/");
      sql.append(database);
      sql.append("','");
      sql.append(username);
      sql.append("','");
      sql.append(password);
      sql.append("','INSERT INTO ");
      sql.append(tableName);
      sql.append("(");
      sql.append(org.apache.commons.lang.StringUtils.join(colNames,','));
      sql.append(") VALUES(");
      sql.append(org.apache.commons.lang.StringUtils.join(valuesCols,','));
      sql.append(")',");
      sql.append(org.apache.commons.lang.StringUtils.join(detailCols,','));
      sql.append(") FROM (SELECT * FROM ");
      sql.append(tableName);
      sql.append(") details");
      try
      {
        stmt.executeQuery(sql.toString());
        status = true;
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      return status;
    }

    /**
    * Stores query into specified hive table
    * @param query
    * @param table name
    * @return true if stored successfully
    */
    public JSONObject executeQuery(String query) throws HiveDAOException
    {
      if(query == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: query=%s", query));
      Statement stmt = connManager.createStatement();
      ResultSet rs = null;
      StringBuffer sql = new StringBuffer(query);

      try
      {
         rs = stmt.executeQuery(sql.toString());
      }
      catch (SQLException e)
      {
         logger.error("unable to execute: " + sql,e);
         throw new HiveDAOException(e,"unable to execute: "+sql);
      }
      finally
      {
         HiveConnectionManager.closeStatement(stmt);
      }
      JSONObject jObject = resultSetToJson(rs);

      return jObject;

    }


    public void closeConnection(){
        connManager.closeConnection();
    }
    }
