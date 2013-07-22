/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

import org.json.simple.JSONObject;

import com.google.gson.JsonObject;

import java.util.Map;

/**
 * 
 * @author nstanford
 *
 */
public interface HiveDAO {

	/**
	 * 
	 * @param connectionManager
	 */
	public void setConnectionManager(HiveConnectionManager connectionManager);
	
	/**
	 * 
	 * @return
	 */
	public HiveConnectionManager getConnectionManager();
	
	/**
	 * 
	 * @param fileName
	 * @param tableName
	 * @param local
	 * @param overwrite
	 */
	public void loadData(String fileName, String tableName, boolean local, boolean overwrite) throws HiveDAOException;
	
	/**
	 * 
	 * @param fileName
	 * @param tableName
	 */
	public void loadData(String fileName, String tableName) throws HiveDAOException;
	
	/**
	 * 
	 * @param jsonFormatedQueries
	 * @throws HiveDAOException
	 */
	public void executeJsonQueries(String jsonFormatedQueries) throws HiveDAOException;
	
	/**
	 * 
	 * @param query
	 * @return 
	 * @throws HiveDAOException
	 */
	public JSONObject executeQuery(String query) throws HiveDAOException;
	
	/**
	 * 
	 */
	public void closeConnection();

    /**
	 * Stores the query results/output to a new table created around the results
	 * @param query
     * @param tablename
	 * @return
	 * @throws HiveDAOException
	 */
    public boolean storeQueryOutputToHiveTable(String query, String tablename) throws HiveDAOException;

    /**
	 * Inserts the query results/output to an existing Hive Table
	 * @param query
     * @param tablename
	 * @return
	 * @throws HiveDAOException
	 */
	public boolean insertQueryOutputToHiveTable(String query, String tablename) throws HiveDAOException;

    /**
	 * Inserts the query results/output to an existing Hive Table with partitions.
	 * @param query
     * @param tableName
	 * @return
	 * @throws HiveDAOException
	 */
	public boolean insertQueryOutputToHiveTable(String query, String tableName, Map partitions ) throws HiveDAOException;

    /**
	 * Inserts the query results/output to an existing Hive Table
	 * @param server
     * @param database
     * @param tableName
	 * @return
	 * @throws HiveDAOException
	 */
	public boolean exportQueryOutputToMySQLTable(String server, String port, String database, String username, String password, String tableName, String[] colName) throws HiveDAOException;

    /**
	 * @param tableName
	 */
	public Map hiveTableColumnsAndTypes(String tableName) throws HiveDAOException;
}
