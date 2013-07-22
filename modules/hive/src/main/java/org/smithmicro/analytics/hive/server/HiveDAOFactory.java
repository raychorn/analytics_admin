/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

import org.apache.log4j.Logger;

/**
 * 
 * @author nstanford
 *
 */
public class HiveDAOFactory {
	
	private static Logger logger = Logger.getLogger(HiveDAOFactory.class);
	
	public enum HiveDAOType{
		ADMIN,BASE,DATAMINING
	}
	
	public static HiveDAO getHiveDAO(final String server, String port, final String dbName, HiveDAOType type) throws HiveDAOException
	{
		HiveConnectionManager connManager = new HiveConnectionManager(server,port,dbName);
		HiveDAO dao = null;
		if( type == HiveDAOType.ADMIN ){
			dao = new HiveAdminImpl();
			dao.setConnectionManager(connManager);
		}
		else if (type == HiveDAOType.BASE)
      {
         dao = new HiveBaseImpl();
         dao.setConnectionManager(connManager);
      }
		
		return dao;
	}
}

