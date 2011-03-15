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
    private final String connectionUrl;
    private final String hostname;
    private final String login;
    private final String password;
    private final String dbName;
    private final String sqlQuery;
    private final String authZQuery;
    private final String googleConnectorWorkDir;
    private final String[] primaryKeys;
    private final String xslt;
    private final String driverClassName;
    private String documentURLField;
    private String documentIdField;
    private String baseURL;
    private String lobField;
    private String fetchURLField;
    private String lastModifiedDate;
    private String documentTitle;
    private String extMetadataType;
	private int NO_OF_ROWS = 200;
    private boolean publicFeed = true;

    public DBContext(String connectionUrl, String hostname,
            String driverClassName, String login, String password,
            String dbName, String sqlQuery, String googleConnectorWorkDir,
            String primaryKeysString, String xslt, String authZQuery,
            String lastModifiedDate, String documentTitle,
            String documentURLField, String documentIdField, String baseURL,
            String lobField, String fetchURLField, String extMetadataType,
            String noOfRows) {

        this.connectionUrl = connectionUrl;
        this.hostname = hostname;
        this.driverClassName = driverClassName;
        this.login = login;
        this.password = password;
        this.dbName = dbName;
        this.sqlQuery = sqlQuery;
        this.googleConnectorWorkDir = googleConnectorWorkDir;
        this.primaryKeys = primaryKeysString.split(Util.PRIMARY_KEYS_SEPARATOR);;
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
        try {
            this.NO_OF_ROWS = Integer.parseInt(noOfRows);
        } catch (Exception e) {
			LOG.warning("Number Format Exception while setting the no of rows to be fetched");
        }

    }

    public int getNO_OF_ROWS() {
        return NO_OF_ROWS;
    }

    public void setNO_OF_ROWS(int nOOFROWS) {
        NO_OF_ROWS = nOOFROWS;
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

	public String[] getPrimaryKeys() {
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
