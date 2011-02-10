package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.util.diffing.DocumentHandleFactory;
import com.google.enterprise.connector.util.diffing.DocumentSnapshotFactory;


	/**
	 * Top-level factory for Database Connector objects
	 */
	

public class DBClassFactory implements DocumentHandleFactory,
		DocumentSnapshotFactory {
	public static DBClassFactory INSTANCE = new DBClassFactory();

	  public static DBClassFactory getInstance() {
	    return INSTANCE;
	  }

	@Override
	public DBClass fromString(String stringForm) {
		// TODO Auto-generated method stub
		return new DBClass(stringForm);
	}

}
