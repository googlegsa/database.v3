// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import java.util.logging.Logger;

/**
 * An encapsulation of all the config needed for a working Database Connector
 * instance.
 */
public class DBContext {
	private static final Logger LOG = Logger.getLogger(DBContext.class.getName());
	private String connectionUrl;
	private String hostname;
	private String login;
	private String password;
	private String dbName;
	private String sqlQuery;
	private String authZQuery;
	private String googleConnectorWorkDir;
	private String primaryKeys;
	private String xslt;
	private String driverClassName;
	private String documentURLField;
	private String documentIdField;
	private String baseURL;
	private String lobField;
	private String fetchURLField;
	private String lastModifiedDate;
	private String documentTitle;
	private String extMetadataType;
	private int numberOfRows = 200;
	private boolean publicFeed = true;

	public DBContext() {
	}

	public DBContext(String connectionUrl, String hostname,
			String driverClassName, String login, String password, String dbName,
			String sqlQuery, String googleConnectorWorkDir, String primaryKeysString,
			String xslt, String authZQuery, String lastModifiedDate,
			String documentTitle, String documentURLField, String documentIdField,
			String baseURL, String lobField, String fetchURLField,
			String extMetadataType) {

		this.connectionUrl = connectionUrl;
		this.hostname = hostname;
		this.driverClassName = driverClassName;
		this.login = login;
		this.password = password;
		this.dbName = dbName;
		this.sqlQuery = sqlQuery;
		this.googleConnectorWorkDir = googleConnectorWorkDir;
		this.primaryKeys = primaryKeysString;
		this.xslt = xslt;
		this.authZQuery = authZQuery;
		this.extMetadataType = extMetadataType;
		this.documentURLField = documentURLField;
		this.documentIdField = documentIdField;
		this.baseURL = baseURL;
		this.lobField = lobField;
		this.fetchURLField = fetchURLField;
		this.lastModifiedDate = lastModifiedDate;
		this.documentTitle = documentTitle;
	}

	public static Logger getLog() {
		return LOG;
	}

	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setSqlQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
	}

	public void setAuthZQuery(String authZQuery) {
		this.authZQuery = authZQuery;
	}

	public void setGoogleConnectorWorkDir(String googleConnectorWorkDir) {
		this.googleConnectorWorkDir = googleConnectorWorkDir;
	}

	public void setPrimaryKeys(String primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public void setXslt(String xslt) {
		this.xslt = xslt;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public void setDocumentURLField(String documentURLField) {
		this.documentURLField = documentURLField;
	}

	public void setDocumentIdField(String documentIdField) {
		this.documentIdField = documentIdField;
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	public void setLobField(String lobField) {
		this.lobField = lobField;
	}

	public void setFetchURLField(String fetchURLField) {
		this.fetchURLField = fetchURLField;
	}

	public void setLastModifiedDate(String lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public void setDocumentTitle(String documentTitle) {
		this.documentTitle = documentTitle;
	}

	public int getNumberOfRows() {
		return numberOfRows;
	}

	public void setNumberOfRows(int numberOfRows) {
		try {
			this.numberOfRows = numberOfRows;
		} catch (NumberFormatException e) {
			LOG.warning("Number Format Exception while setting number of rows to be fetched"
					+ "\n" + e.toString());
		}
	}

	public String getGoogleConnectorWorkDir() {
		return googleConnectorWorkDir;
	}

	public void setExtMetadataType(String extMetadataType) {
		this.extMetadataType = extMetadataType;
	}

	public String getConnectionUrl() {
		return connectionUrl;
	}

	public String getHostname() {
		return hostname;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

	public String getAuthZQuery() {
		return authZQuery;
	}

	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public String getXslt() {
		return xslt;
	}

	public String getLogin() {
		return login;
	}

	public String getPassword() {
		return password;
	}

	public String getDbName() {
		return dbName;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public String getLastModifiedDate() {
		return lastModifiedDate;
	}

	public String getDocumentTitle() {
		return documentTitle;
	}

	public String getExtMetadataType() {
		return extMetadataType;
	}

	public String getDocumentURLField() {
		return documentURLField;
	}

	public String getDocumentIdField() {
		return documentIdField;
	}

	public String getBaseURL() {
		return baseURL;
	}

	public String getLobField() {
		return lobField;
	}

	public String getFetchURLField() {
		return fetchURLField;
	}

	public boolean isPublicFeed() {
		return publicFeed;
	}

	public void setPublicFeed(boolean publicFeed) {
		this.publicFeed = publicFeed;
	}
}
