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

import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import junit.framework.TestCase;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.text.Collator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This is a base class for all test classes that requires interaction with
 * database. This provide methods to interact with database.
 */
public abstract class DBTestBase extends TestCase {

  protected Map<String, String> configMap = new HashMap<String, String>();

  public static final String CREATE_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/createTable.sql";
  public static final String LOAD_TEST_DATA = "com/google/enterprise/connector/db/config/loadTestData.sql";
  public static final String TRUNCATE_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/truncateTestTable.sql";
  public static final String DROP_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/dropdb.sql";

  public static final String CREATE_USER_DOC_MAP_TABLE = "com/google/enterprise/connector/db/config/createUerDocMapTable.sql";
  public static final String LOAD_USER_DOC_MAP_TEST_DATA = "com/google/enterprise/connector/db/config/loadUerDocMapTable.sql";
  public static final String DROP_USER_DOC_MAP_TABLE = "com/google/enterprise/connector/db/config/dropUserDocMapTable.sql";

  // Keep an open connection to H2 to prevent in-memory DB from getting deleted.
  private SqlSession sqlSession = null;
  private Connection dbConnection = null;

  @Override
  protected void setUp() throws Exception {
    TestDirectoryManager testDirManager = new TestDirectoryManager(this);
    configMap.put("login", LanguageResource.getPropertyValue("login"));
    configMap.put("password", LanguageResource.getPropertyValue("password"));
    configMap.put("connectionUrl", LanguageResource.getPropertyValue("connectionUrl"));
    configMap.put("googleConnectorName", "test_connector");
    configMap.put("driverClassName", LanguageResource.getPropertyValue("driverClassName"));
    configMap.put("sqlQuery", LanguageResource.getPropertyValue("sqlQuery"));
    configMap.put("primaryKeysString", LanguageResource.getPropertyValue("primaryKeysString"));
    configMap.put("googleConnectorWorkDir", testDirManager.getTmpDir());
    configMap.put("xslt", "");
    configMap.put("authZQuery", LanguageResource.getPropertyValue("authZQuery"));
    configMap.put("lastModifiedDate", "");
    configMap.put("documentTitle", "title");
    configMap.put("documentURLField", "docURL");
    configMap.put("documentIdField", "docId");
    configMap.put("baseURL", "http://myhost/app/");
    configMap.put("lobField", "lob");
    configMap.put("fetchURLField", "fetchURL");
    configMap.put("extMetadataType", "");
    sqlSession = getDbClient().getSqlSession();
    dbConnection = sqlSession.getConnection();
  }

  @Override
  protected void tearDown() throws Exception {
    if (dbConnection != null) {
      dbConnection.close();
      dbConnection = null;
    }
    if (sqlSession != null) {
      sqlSession.close();
      sqlSession = null;
    }
  }

  protected ProductionTraversalContext getProductionTraversalContext() {
    ProductionTraversalContext context = new ProductionTraversalContext();
    return context;
  }

  protected DBContext getDbContext() {
    return getDbContext(configMap);
  }

  protected DBContext getDbContext(Map<String, String> configMap) {
    DBContext dbContext = new DBContext();
    dbContext.setClient(new DBClient());
    dbContext.setConnectionUrl(configMap.get("connectionUrl"));
    dbContext.setGoogleConnectorName(configMap.get("googleConnectorName"));
    dbContext.setDriverClassName(configMap.get("driverClassName"));
    dbContext.setLogin(configMap.get("login"));
    dbContext.setPassword(configMap.get("password"));
    dbContext.setSqlQuery(configMap.get("sqlQuery"));
    dbContext.setGoogleConnectorWorkDir(
        configMap.get("googleConnectorWorkDir"));
    dbContext.setPrimaryKeys(configMap.get("primaryKeysString"));
    dbContext.setXslt(configMap.get("xslt"));
    dbContext.setAuthZQuery(configMap.get("authZQuery"));
    dbContext.setLastModifiedDate(configMap.get("lastModifiedDate"));
    dbContext.setDocumentURLField(configMap.get("documentURLField"));
    dbContext.setDocumentIdField(configMap.get("documentIdField"));
    dbContext.setBaseURL(configMap.get("baseURL"));
    dbContext.setLobField(configMap.get("lobField"));
    dbContext.setFetchURLField(configMap.get("fetchURLField"));
    dbContext.setExtMetadataType(configMap.get("extMetadataType"));
    dbContext.setNumberOfRows(2);

    Collator collator = Collator.getInstance(Locale.US);
    collator.setStrength(Collator.IDENTICAL);
    dbContext.setCollator(collator);

    // Since we're not Spring-instantiated here, we need to explicitly
    // call the init method. Wrap a rare exception to avoid requiring
    // throws clauses everywhere.
    try {
      dbContext.init();
    } catch (DBException e) {
      throw new RuntimeException(e);
    }
    return dbContext;
  }

  public static DBContext getMinimalDbContext() {
      DBContext dbContext = new DBContext();
      dbContext.setPrimaryKeys("id,lastname");
      dbContext.setGoogleConnectorName("test_connector");
      return dbContext;
  }

  protected DBClient getDbClient() {
    return getDbContext().getClient();
  }

  /**
   * Executes the database script.
   *
   * @param scriptPath path of SQL script file
   */
  protected void runDBScript(String scriptPath) throws Exception {
    ScriptRunner runner = new ScriptRunner(dbConnection);
    runner.setStopOnError(true);
    runner.runScript(Resources.getResourceAsReader(scriptPath));
  }
}
