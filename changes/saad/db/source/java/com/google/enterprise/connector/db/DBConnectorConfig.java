package com.google.enterprise.connector.db;

import java.util.logging.Logger;

/**
 * An encapsulation of all the configuration needed for a working Database Connector
 * instance.
 * 
 */
public class DBConnectorConfig {

	private final DBClient dbClient;
	private final DBContext dbContext;
	private final String xslt;
	private static final Logger LOG = Logger.getLogger(DBConnectorConfig.class.getName());

	/**
	 * Sole constructor. This is the injection point for stored configuration, via the
	 * connectorInstance.xml.
	 */
	public DBConnectorConfig(String connectionUrl, String hostname,
			String driverClassName, String login, String password,
			String dbName,String sqlQuery,String googleConnectorWorkDir,String primaryKeys,String xslt, String authZQuery,
			String lastModifiedDate, String documentTitle,
			String documentURLField, String documentIdField, String baseURL,
			String lobField, String fetchURLField, String extMetadataType) throws DBException {
		this.dbContext=new DBContext(connectionUrl, hostname, driverClassName, login, password, dbName, lastModifiedDate, documentTitle, documentURLField, documentIdField, baseURL, lobField, fetchURLField, extMetadataType);
		this.dbClient=new DBClient(dbContext, sqlQuery, googleConnectorWorkDir, primaryKeys.split(Util.PRIMARY_KEYS_SEPARATOR), authZQuery);
		this.xslt=xslt;
	}

	/**
	 * @return dbClient
	 */
	public DBClient getDbClient() {
		return dbClient;
	}

	/**
	 * @return dbContext
	 */
	public DBContext getDbContext() {
		return dbContext;
	}

	/**
	 * @return xslt
	 */
	public String getXslt() {
		return xslt;
	}


}
