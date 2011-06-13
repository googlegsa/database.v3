// Copyright 2009 Google Inc.
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

import com.google.enterprise.connector.spi.Connector;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.Session;

import java.util.Map;

/**
 * Implementation of {@link Connector} for the database connector. This provides
 * session to the connector manager.
 */

public class DBConnector implements Connector {
  private final DBContext dbContext;
  private final String sqlQuery;
  private final String authZQuery;
  private final String googleConnectorWorkDir;
  private final String primaryKeysString;
  private final String xslt;
  private Map<String, String> dbTypeDriver = null;

  public DBConnector(String connectionUrl, String hostname,
    String driverClassName, String login, String password, String dbName,
    String sqlQuery, String googleConnectorWorkDir, String primaryKeysString,
    String xslt, String authZQuery, String lastModifiedDate,
    String documentURLField, String documentIdField, String baseURL,
    String lobField, String fetchURLField, String extMetadataType) {

    this.dbContext = new DBContext(connectionUrl, hostname, driverClassName,
        login, password, dbName, lastModifiedDate, documentURLField,
        documentIdField, baseURL, lobField, fetchURLField, extMetadataType);
    this.sqlQuery = sqlQuery;
    this.googleConnectorWorkDir = googleConnectorWorkDir;
    this.primaryKeysString = primaryKeysString;
    this.xslt = xslt;
    this.authZQuery = authZQuery;
  }

  public Map<String, String> getDbTypeDriver() {
    return dbTypeDriver;
  }

  /* @Override */
  public Session login() throws RepositoryException {
    DBClient dbClient;
    try {
      dbClient = new DBClient(dbContext, sqlQuery, googleConnectorWorkDir,
          primaryKeysString.split(Util.PRIMARY_KEYS_SEPARATOR), authZQuery);
      return new DBSession(dbClient, xslt);
    } catch (DBException e) {
      throw new RepositoryException("Could not create DB client.", e.getCause());
    }
  }

  public void test() {
    if (true) {
      String ss = new String("test");

    }
  }
}
