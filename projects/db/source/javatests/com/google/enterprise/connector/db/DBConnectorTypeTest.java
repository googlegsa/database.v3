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

import com.google.common.collect.Maps;
import com.google.enterprise.connector.spi.ConfigureResponse;

import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

/**
 * Tests for {@link DBConnectorType} class.
 */
public class DBConnectorTypeTest extends DBTestBase {
  private static final Logger LOG =
      Logger.getLogger(DBConnectorTypeTest.class.getName());

  private static final ResourceBundle BUNDLE =
        ResourceBundle.getBundle("config/DbConnectorResources");

  private DBConnectorType connectorType;

  private MockDBConnectorFactory mdbConnectorFactory;

  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);

    // De-populate a bunch of properties for config form testing and validation.
    configMap.put("authZQuery", "");
    configMap.put("baseURL", "");
    configMap.put("documentIdField", "");
    configMap.put("documentURLField", "");
    configMap.put("fetchURLField", "");
    configMap.put("lobField", "");

    connectorType = new DBConnectorType();

    mdbConnectorFactory = new MockDBConnectorFactory();
  }

  public void testMissingRequiredFields() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    // Remove a required field.
    newConfigMap.put("dbName", "");
    newConfigMap.put("sqlQuery", "");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertNotNull(configRes);
    String message = configRes.getMessage();
    assertTrue(message, message.contains(BUNDLE.getString("REQ_FIELDS")));
    // There was a bug where each required field would appear twice in
    // the message.
    int index = message.indexOf(BUNDLE.getString("dbName"));
    assertTrue(message, index > 0);
    index = message.indexOf(BUNDLE.getString("dbName"), index + 1);
    assertFalse(message, index > 0);
    index = message.indexOf(BUNDLE.getString("sqlQuery"));
    assertTrue(message, index > 0);
    index = message.indexOf(BUNDLE.getString("sqlQuery"), index + 1);
    assertFalse(message, index > 0);
  }

  public void testValidConfig() {
    ConfigureResponse configRes = connectorType.validateConfig(configMap,
        Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  public void testUpdateQuery() throws Exception {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    newConfigMap.put("sqlQuery", "update TestEmpTable set dept = 42");
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals(BUNDLE.getString("TEST_SQL_QUERY"), configRes.getMessage());

    // Verify that the table was not really updated.
    assertEmptyResultSet("select id from TestEmpTable where dept = 42");
  }

  /** Asserts that the ResultSet from the given query has no rows. */
  private void assertEmptyResultSet(String sqlQuery) throws SQLException {
    assertFalse(applyResultSet("select id from TestEmpTable where dept = 42",
        new DBClient.SqlFunction<ResultSet, Boolean>() {
          public Boolean apply(ResultSet resultSet) throws SQLException {
            return resultSet.next();
          }
        }));
  }

  /**
   * Executes the given query and applies the given function to the
   * result set.
   *
   * @return the value of the function applied to the {@code ResultSet}
   */
  private <T> T applyResultSet(String sqlQuery,
      DBClient.SqlFunction<ResultSet, T> function) throws SQLException {
    SqlSession sqlSession = getDbClient().getSqlSession();
    try {
      Connection dbConnection = sqlSession.getConnection();
      try {
        Statement stmt = dbConnection.createStatement();
        try {
          ResultSet resultSet = stmt.executeQuery(sqlQuery);
          try {
            return function.apply(resultSet);
          } finally {
            resultSet.close();
          }
        } finally {
          stmt.close();
        }
      } finally {
        dbConnection.close();
      }
    } finally {
      sqlSession.close();
    }
  }

  public void testInvalidQuery() {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    newConfigMap.put("sqlQuery", "choose the red pill");
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals(BUNDLE.getString("TEST_SQL_QUERY"), configRes.getMessage());
  }

  /**
   * Scenario when more than one primary key is provided and connector is
   * configured for using the parameterized crawl query.
   */
  public void testParameterizedQueryAndMutiplePrimaryKeys() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put("sqlQuery",
                     "select * from TestEmpTable where id > #{value}");
    newConfigMap.put("primaryKeysString", "id,fname");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals(
        BUNDLE.getString("TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER"),
        configRes.getMessage());
  }

  /**
   * Scenario when single primary key is provided and connector is configured
   * for using the parameterized crawl query.
   */
  public void testParameterizedQueryAndSinglePrimaryKeys() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put("sqlQuery", "select * from TestEmpTable where id > #{value}");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  /** Tests an authZ query with no #{username} placeholder. */
  public void testInvalidAuthZQuery() throws Exception {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    newConfigMap.put("authZQuery", "choose the red pill");
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals(BUNDLE.getString("INVALID_AUTH_QUERY"),
        configRes.getMessage());
  }

  /**
   * Tests an authZ query with a quoted #{username} placeholder. This
   * should fail, but does not because the validation parameter
   * replacement is too simple.
   */
  public void testQuotedPlaceholdersAuthZQuery() throws Exception {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    // TODO(jlacey): This query is mangled so that the broken
    // string-valued ${docIds} will work with H2. This should be
    // restored to test ${docIds} against id instead of lname once
    // that is fixed.
    newConfigMap.put("authZQuery", "select id from TestEmpTable where "
        + "lname = '#{username}' and lname IN (${docIds})");
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  public void testAuthZUpdateQuery() throws Exception {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    // TODO(jlacey): See TODO in testQuotedPlaceholdersAuthZQuery.
    newConfigMap.put("authZQuery", "update TestEmpTable set dept = 42"
        + "where lname <> '#{username}' or not lname IN (${docIds})");
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals(BUNDLE.getString("INVALID_AUTH_QUERY"),
        configRes.getMessage());

    // Verify that the table was not really updated.
    assertEmptyResultSet("select id from TestEmpTable where dept = 42");
  }

  /**
   * Test for getConfigForm method.
   */
  public void testGetConfigForm() {
    final ConfigureResponse configureResponse =
        connectorType.getConfigForm(Locale.ENGLISH);
    final String configForm = configureResponse.getFormSnippet();
    assertExpectedFields(configForm);
  }

  /**
   * Test for getPopulatedConfigForm method.
   */
  public void testGetPopulatedConfigForm() {
    final ConfigureResponse configureResponse =
        connectorType.getPopulatedConfigForm(configMap, Locale.ENGLISH);
    final String configForm = configureResponse.getFormSnippet();
    assertExpectedFields(configForm);
  }

  /**
   * Tests expected patterns in html text of configuration form.
   *
   * @param configForm is html string of configuration form for database
   *        connector
   */
  private void assertExpectedFields(final String configForm) {
    LOG.info("Checking for Sql Query field..." + "\n" + configForm);
    String strPattern = "<textarea .*name=\"sqlQuery\".*>";
    Pattern pattern = Pattern.compile(strPattern);
    Matcher match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Driver Class Name field...");
    strPattern = "<input.*size=\"50\" name=\"driverClassName\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Password field...");
    strPattern = "<input.*type=\"password\".*size=\"50\" name=\"password\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Primary Keys String field...");
    strPattern = "<input.*size=\"50\" name=\"primaryKeysString\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for login field...");
    strPattern = "<input.*type=\"text\".*size=\"50\" name=\"login\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Database Name field...");
    strPattern = "<input.*size=\"50\" name=\"dbName\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for xslt field...");
    strPattern = "<textarea .*name=\"xslt\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for 'authZ Query' field...");
    strPattern = "<textarea .*name=\"authZQuery\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Hostname field...");
    strPattern = "<input.*size=\"50\" name=\"hostname\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Connection URL field...");
    strPattern = "<input.*size=\"50\" name=\"connectionUrl\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for radio buttons...");
    strPattern = "<input.*type='radio'.*name='extMetadataType'.*value='url'.*"
        + "onClick='.*'/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    strPattern = "<input type='radio'.*name='extMetadataType'.*value='docId'.*"
        + "onClick='.*'/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    strPattern = "<input type='radio'.*name='extMetadataType' value='lob'.*"
        + "onClick='.*'/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Last Modified date Field...");
    strPattern = "<input.*size=\"50\".*name=\"lastModifiedDate\".*"
        + "id=\"lastModifiedDate\".*/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());
  }
}
