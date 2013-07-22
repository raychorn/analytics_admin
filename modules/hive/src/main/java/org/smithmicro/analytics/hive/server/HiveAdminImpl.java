/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.gson.JsonObject;

/**
 * 
 * @author nstanford
 *
 */
public class HiveAdminImpl extends HiveBaseImpl implements HiveAdminDAO {

	private static Logger logger = Logger.getLogger(HiveAdminImpl.class);
	
	@Override
	public void dropTable(final String tableName) throws HiveDAOException {
		if(tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s",tableName));
        Statement stmt = connManager.createStatement();
		StringBuffer sql = new StringBuffer("drop table ");
		sql.append(tableName);
		try{
			stmt.execute(sql.toString());
		}catch(SQLException e){
			logger.error("unable to drop table " + tableName,e);
			throw new HiveDAOException(e,"unable to drop table "+tableName);
		}finally{
			HiveConnectionManager.closeStatement(stmt);
		}
	}

	@Override
	public String showTables() throws HiveDAOException {
		Statement stmt = connManager.createStatement();
		String sql = "show tables ";
		ResultSet rs = null;
		JSONObject jsonResult = new JSONObject();
		try{
			rs = stmt.executeQuery(sql);
			jsonResult = resultSetToJson(rs); 
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to show tables.");
		}finally{
			HiveConnectionManager.closeConnections(null, stmt, rs);
		}
		
		return jsonResult.toString();
	}

	@Override
	public String describeTable(final String tableName) throws HiveDAOException {
		if(tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s",tableName));
        Statement stmt = connManager.createStatement();
		String sql = "describe "+tableName;
		ResultSet rs = null;
		JSONObject jsonResult = new JSONObject();
		try {
			rs = stmt.executeQuery(sql);
			jsonResult = resultSetToJson(rs);
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to describe table "+tableName);
		}finally{
			HiveConnectionManager.closeConnections(null, stmt, rs);
		}

        return jsonResult.toString();

	}

	@Override
	public String selectAllRows(final String tableName) throws HiveDAOException {
		if(tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s",tableName));
        Statement stmt = connManager.createStatement();
		String sql = "select * from "+tableName;
		ResultSet rs = null;
		JSONObject jsonResult = new JSONObject();
		try {
			rs = stmt.executeQuery(sql);
			jsonResult = resultSetToJson(rs);
		}catch(Exception e){
			throw new HiveDAOException(e,"unable execute select * from "+tableName);
		}finally{
			HiveConnectionManager.closeConnections(null, stmt, rs);
		}
		
		return jsonResult.toString();
	}

	@Override
	public String getNumberOfRows(final String tableName) throws HiveDAOException {
		if(tableName == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s",tableName));
        Statement stmt = connManager.createStatement();
		String sql = "select count(*) from "+tableName;
		ResultSet rs = null;
		JSONObject jsonResult = new JSONObject();
		try {
			rs = stmt.executeQuery(sql);
			jsonResult = resultSetToJson(rs);
		}catch(Exception e){
			throw new HiveDAOException(e,"unable execute select count(*) from "+tableName);
		}finally{
			HiveConnectionManager.closeConnections(null, stmt, rs);
		}
		return jsonResult.values().toArray()[0].toString();
	}

	@Override
	public void createTable(final String tableName, final List<String> colNames, final List<String> colTypes) throws HiveDAOException {

       if(tableName == null || colNames == null || colTypes == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s, colNames=%s, colTypes=%s",
                tableName,colNames,colTypes));
		if( colNames.size() != colTypes.size()){
			logger.error("number of column names does not equal the number of column types.");
			throw new HiveDAOException("number of column names does not equal the number of column types.");
		}
		StringBuffer sql = new StringBuffer("create table ");
		sql.append(tableName);
		sql.append(" (");
		boolean first = true;
		int index = 0;
		for(String currentColName : colNames){
			if( !first )
				sql.append(",");
			first = false;
			sql.append(currentColName);
			sql.append(" ");
			sql.append(colTypes.get(index));
			index++;
		}
		sql.append(" )");
		logger.info("executing create table statment: " + sql.toString());
		Statement stmt = connManager.createStatement();
		try {
			stmt.execute(sql.toString());
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to execute: "+ sql.toString());
		}
	}

    @Override
	public void createTable(final String tableName, final List<String> colNames, final List<String> colTypes, final Map partColNamesTypes) throws HiveDAOException {
		if(tableName == null || colNames == null || colTypes == null || partColNamesTypes == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s, colNames=%s, colTypes=%s, partColNamesTypes=%s",
                tableName,colNames,colTypes, partColNamesTypes));
        if( colNames.size() != colTypes.size()){
			logger.error("number of column names does not equal the number of column types.");
			throw new HiveDAOException("number of column names does not equal the number of column types.");
		}
		StringBuffer sql = new StringBuffer("create table ");
		sql.append(tableName);
		sql.append(" (");
		boolean first = true;
		int index = 0;
		for(String currentColName : colNames){
			if( !first )
				sql.append(",");
			first = false;
			sql.append(currentColName);
			sql.append(" ");
			sql.append(colTypes.get(index));
			index++;
		}
		sql.append(" )");
        if ( partColNamesTypes.size() > 0)
        {
            sql.append(" PARTITIONED BY (");
            Iterator it = partColNamesTypes.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry colNameValues = (Map.Entry)it.next();
                String partColName = colNameValues.getKey().toString();
                String partColType = colNameValues.getValue().toString();
                if ( !partColName.equals("") && !partColType.equals(""))
                    sql.append(partColName + " " + partColType);
                if (it.hasNext())
                    sql.append(", ");
                else
                    sql.append(") ");
            }
        }
		logger.info("executing create table statement with partitions: " + sql.toString());
		Statement stmt = connManager.createStatement();
		try {
			stmt.execute(sql.toString());
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to execute: "+ sql.toString());
		}
	}
    @Override
	public void addPartitionToTable(final String tableName, final Map partColNamesTypes) throws HiveDAOException {
        if(tableName == null || partColNamesTypes == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s, partColNamesTypes=%s",tableName, partColNamesTypes));
        StringBuffer sql = new StringBuffer("alter table ");
        try {
            Statement stmt = connManager.createStatement();
            sql.append(tableName);

            if ( partColNamesTypes.size() > 0)
            {
                sql.append(" ADD IF NOT EXISTS PARTITION (");
                Iterator it = partColNamesTypes.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry colNameValues = (Map.Entry)it.next();
                    String partColName = colNameValues.getKey().toString();
                    String partColType = colNameValues.getValue().toString();
                    if ( colNameValues.getValue() instanceof String)
                        partColType = "'" + partColType +"'" ;
                    if ( !partColName.equals("") && !partColType.equals(""))
                        sql.append(partColName + " = " + partColType);
                    if (it.hasNext())
                        sql.append(", ");
                    else
                        sql.append(") ");
                }
            }
            logger.info("executing alter table statement with partitions: " + sql.toString());
            stmt.execute(sql.toString());
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to execute: "+ sql.toString());
		}
	}

    @Override
	public void dropPartitionFromTable(final String tableName, final Map partColNamesTypes) throws HiveDAOException {
        if(tableName == null || partColNamesTypes == null)
         throw new IllegalArgumentException(String.format("Parameters can't " +
               "be null: tableName=%s, partColNamesTypes=%s",tableName, partColNamesTypes));
        StringBuffer sql = new StringBuffer("alter table ");
        try {
            Statement stmt = connManager.createStatement();
            sql.append(tableName);

            if ( partColNamesTypes.size() > 0)
            {
                sql.append(" DROP IF EXISTS PARTITION (");
                Iterator it = partColNamesTypes.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry colNameValues = (Map.Entry)it.next();
                    String partColName = colNameValues.getKey().toString();
                    String partColType = colNameValues.getValue().toString();
                    if ( colNameValues.getValue() instanceof String)
                        partColType = "'" + partColType +"'";
                    if ( !partColName.equals("") && !partColType.equals(""))
                        sql.append(partColName + " = " + partColType);
                    if (it.hasNext())
                        sql.append(", ");
                    else
                        sql.append(") ");
                }
            }
            logger.info("executing alter table statement with partitions: " + sql.toString());
            stmt.execute(sql.toString());
		}catch(Exception e){
			throw new HiveDAOException(e,"unable to execute: "+ sql.toString());
		}
	}
}
