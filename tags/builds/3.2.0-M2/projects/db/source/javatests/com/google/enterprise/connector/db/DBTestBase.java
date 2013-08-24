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

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

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
    configMap.put("dbName", LanguageResource.getPropertyValue("dbName"));
    configMap.put("hostname", LanguageResource.getPropertyValue("hostname"));
    configMap.put("driverClassName", LanguageResource.getPropertyValue("driverClassName"));
    configMap.put("sqlQuery", LanguageResource.getPropertyValue("sqlQuery"));
    configMap.put("primaryKeysString", LanguageResource.getPropertyValue("primaryKeysString"));
    configMap.put("googleConnectorWorkDir", testDirManager.getTmpDir());
    configMap.put("xslt", "");
    configMap.put("authZQuery", LanguageResource.getPropertyValue("authZQuery"));
    configMap.put("lastModifiedDate", "");
    configMap.put("documentTitle", "title");
    configMap.put("externalMetadata", "");
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
    try {
      DBContext dbContext = new DBContext(configMap.get("connectionUrl"),
          configMap.get("hostname"), configMap.get("driverClassName"),
          configMap.get("login"), configMap.get("password"),
          configMap.get("dbName"), configMap.get("sqlQuery"),
          configMap.get("googleConnectorWorkDir"),
          configMap.get("primaryKeysString"), configMap.get("xslt"),
          configMap.get("authZQuery"), configMap.get("lastModifiedDate"),
          configMap.get("documentTitle"), configMap.get("documentURLField"),
          configMap.get("documentIdField"), configMap.get("baseURL"),
          configMap.get("lobField"), configMap.get("fetchURLField"),
          configMap.get("extMetadataType"));
      dbContext.setNumberOfRows(2);
      return dbContext;
    } catch (DBException e) {
      // Wrap a rare exception to avoid requiring throws clauses everywhere.
      throw new RuntimeException(e);
    }
  }

  public static DBContext getMinimalDbContext() {
      DBContext dbContext = new DBContext();
      dbContext.setDbName("testdb_");
      dbContext.setPrimaryKeys("id,lastname");
      dbContext.setHostname("localhost");
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
