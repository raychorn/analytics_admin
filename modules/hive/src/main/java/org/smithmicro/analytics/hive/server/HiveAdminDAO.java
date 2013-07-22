/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */

/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

import java.util.List;
import java.util.Map;

/**
 * 
 * @author nstanford
 *
 */
public interface HiveAdminDAO extends HiveDAO {

	/**
	 * 
	 * @param tableName
	 * @throws HiveDAOException
	 */
	public void dropTable(String tableName) throws HiveDAOException;
	
	/**
	 * 
	 * @return
	 * @throws HiveDAOException
	 */
	public String showTables() throws HiveDAOException;
	
	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws HiveDAOException
	 */
	public String describeTable(String tableName) throws HiveDAOException;
	
	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws HiveDAOException
	 */
	public String selectAllRows(String tableName) throws HiveDAOException;
	
	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws HiveDAOException
	 */
	public String getNumberOfRows(String tableName) throws HiveDAOException;

	/**
	 * 
	 * @param tableName
	 * @param colName
	 * @param colTypes
	 * @throws HiveDAOException
	 */
	public void createTable(String tableName,  final List<String> colName, final List<String> colTypes) throws HiveDAOException;

    /**
	 * Create Table with partitions.
	 * @param tableName
	 * @param colName
	 * @param colTypes
     * @param partColNamesTypes
	 * @throws HiveDAOException
	 */
    public void createTable(String tableName,  final List<String> colName, final List<String> colTypes, final Map partColNamesTypes) throws HiveDAOException;

    /**
	 * Alter the Table to add partitions
	 * @param tableName
	 * @param partColNamesTypes
	 * @throws HiveDAOException
	 */
	public void addPartitionToTable(String tableName,  final Map partColNamesTypes) throws HiveDAOException;

    /**
	 * Alter the Table to drop partitions
	 * @param tableName
	 * @param partColNamesTypes
	 * @throws HiveDAOException
	 */
	public void dropPartitionFromTable(String tableName,  final Map partColNamesTypes) throws HiveDAOException;
}
