// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import com.google.common.collect.ImmutableList;

import java.text.Collator;
import java.util.Set;

/**
 * An encapsulation of all the config needed for a working Database Connector
 * instance.
 */
public class DBContext implements ValueOrdering {
  private DBClient client;
  private String connectionUrl;
  private String connectorName;
  private String login;
  private String password;
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
  private String extMetadataType;
  private int numberOfRows = 500;
  private Integer minValue = -1;
  private boolean publicFeed = true;
  private boolean parameterizedQueryFlag = false;
  private Boolean nullsSortLow = null;
  private Collator collator;

  public DBContext() {
  }

  public void init() throws DBException {
    client.setDBContext(this);

    // If the NULL value sort behaviour has not been explicitly overriden
    // in the configuration, fetch it from the DatabaseMetadata.
    if (nullsSortLow == null) {
      nullsSortLow = client.nullsAreSortedLow();
      if (nullsSortLow == null) {
        throw new DBException("nullsSortLowFlag must be set in configuration.");
      }
    }
  }

  public boolean isParameterizedQueryFlag() {
    return parameterizedQueryFlag;
  }

  public void setParameterizedQueryFlag(boolean parameterizedQueryFlag) {
    this.parameterizedQueryFlag = parameterizedQueryFlag;
  }

  public void setClient(DBClient client) {
    this.client = client;
  }

  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  public void setGoogleConnectorName(String connectorName) {
    this.connectorName = connectorName;
  }

  public void setLogin(String login) {
    this.login = login;
  }

  public void setPassword(String password) {
    this.password = password;
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

  public void setPrimaryKeys(String primaryKeysString) {
    this.primaryKeys = primaryKeysString;
  }

  public void setXslt(String xslt) {
    this.xslt = xslt;
  }

  public void setDriverClassName(String driverClassName) {
    this.driverClassName = driverClassName;
  }

  public void setDocumentURLField(String documentURLField) {
    this.documentURLField = Util.nullOrTrimmed(documentURLField);
  }

  public void setDocumentIdField(String documentIdField) {
    this.documentIdField = Util.nullOrTrimmed(documentIdField);
  }

  public void setBaseURL(String baseURL) {
    this.baseURL = baseURL;
  }

  public void setLobField(String lobField) {
    this.lobField = Util.nullOrTrimmed(lobField);
  }

  public void setFetchURLField(String fetchURLField) {
    this.fetchURLField = Util.nullOrTrimmed(fetchURLField);
  }

  public void setLastModifiedDate(String lastModifiedDate) {
    this.lastModifiedDate = Util.nullOrTrimmed(lastModifiedDate);
  }

  public int getNumberOfRows() {
    return numberOfRows;
  }

  public void setNumberOfRows(int numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  public Integer getMinValue() {
    return minValue;
  }

  public void setMinValue(Integer minValue) {
    this.minValue = minValue;
  }

  public String getGoogleConnectorWorkDir() {
    return googleConnectorWorkDir;
  }

  public void setExtMetadataType(String extMetadataType) {
    this.extMetadataType = extMetadataType;
  }

  public DBClient getClient() {
    return client;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public String getAuthZQuery() {
    return authZQuery;
  }

  /*
   * This must have a different name or access from setPrimaryKeys to
   * avoid a Spring bean introspection error with string vs list.
   */
  public ImmutableList<String> getPrimaryKeyColumns(Set<String> columnNames)
      throws DBException {
    ImmutableList<String> matchedColumns =
        Util.getCanonicalPrimaryKey(primaryKeys, columnNames);
    if (matchedColumns == null) {
      throw new DBException(
          "Primary Key does not match any of the column names.");
    } else {
      return matchedColumns;
    }
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

  public String getDriverClassName() {
    return driverClassName;
  }

  public String getLastModifiedDate() {
    return lastModifiedDate;
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

  @Override
  public boolean nullsAreSortedLow() {
    return nullsSortLow.booleanValue();
  }  

  public void setNullsAreSortedLow(Boolean nullsSortLow) {
    this.nullsSortLow = nullsSortLow;
  }

  @Override
  public Collator getCollator() {
    return collator;
  }

  public void setCollator(Collator collator) {
    this.collator = collator;
  }
}
