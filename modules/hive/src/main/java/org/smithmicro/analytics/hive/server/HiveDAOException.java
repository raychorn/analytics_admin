/*
 * Copyright (c) 2011. SmithMicro Software, Inc.
 * All Rights Reserved.
 */
package org.smithmicro.analytics.hive.server;

/**
 * 
 * @author nstanford
 *
 */
public class HiveDAOException extends Exception{

	/**
    * 
    */
   private static final long serialVersionUID = 1L;
   private String message;
	
	public HiveDAOException(Exception e){
		super(e);
	}
	
	public HiveDAOException(Exception e, String message){
		super(e);
		this.message = message;
	}
	
	public HiveDAOException(String message){
		this.message = message;
	}
	
	public void setMessage(String message){
		this.message = message;
	}
	
	public String getMessage(){
		return this.message;
	}
}
