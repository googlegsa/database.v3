package com.google.enterprise.connector.db;

import java.util.logging.Logger;

public class DBConnectorConfig {

	private final DBClient dbClient;
	private final DBContext dbContext;
	private final String xslt;
	private static final Logger LOG = Logger.getLogger(DBClient.class.getName());

	public DBConnectorConfig(String connectionUrl, String hostname,
			String driverClassName, String login, String password,
			String dbName,String sqlQuery,String googleConnectorWorkDir,String[] primaryKeys,String xslt, String authZQuery,
			String lastModifiedDate, String documentTitle,
			String documentURLField, String documentIdField, String baseURL,
			String lobField, String fetchURLField, String extMetadataType) throws DBException {

		LOG.info("DBConnectorConfig googleConnectorWorkDir"+googleConnectorWorkDir+"hostname"+hostname);
		LOG.info("sqlqueryis"+sqlQuery);
		this.dbContext=new DBContext(connectionUrl, hostname, driverClassName, login, password, dbName, lastModifiedDate, documentTitle, documentURLField, documentIdField, baseURL, lobField, fetchURLField, extMetadataType);
		this.dbClient=new DBClient(dbContext, sqlQuery, googleConnectorWorkDir, primaryKeys, authZQuery);
		this.xslt=xslt;
	}

	public DBClient getDbClient() {
		return dbClient;
	}

	public DBContext getDbContext() {
		return dbContext;
	}

	public String getXslt() {
		return xslt;
	}


}
