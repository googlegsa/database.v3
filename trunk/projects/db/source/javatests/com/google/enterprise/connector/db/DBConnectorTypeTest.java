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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.enterprise.connector.spi.ConfigureResponse;

import junit.framework.TestCase;

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

  public void testAlwaysRequiredFields() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    // Remove the required fields.
    for (String field : DBConnectorType.ALWAYS_REQUIRED_FIELDS) {
      newConfigMap.put(field, "");
    }
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertNotNull(configRes);
    String message = configRes.getMessage();
    assertTrue(message, message.contains(BUNDLE.getString("REQ_FIELDS")));
    for (String field : DBConnectorType.ALWAYS_REQUIRED_FIELDS) {
      String label = BUNDLE.getString(field);
      int index = message.indexOf(label);
      assertFalse(message + " does not contain " + label, index == -1);

      // There was a bug where each required field would appear twice in
      // the message.
      index = message.indexOf(label, index + 1);
      assertTrue(message + " contains duplicates of " + label, index == -1);
    }
  }

  /**
   * If none of the sometimes-required fields are set, or actually if
   * none of them are set for the given value of extMetadataType,
   * that's current OK. The connector will fall back to noExt, that
   * is, the stylesheet.
   */
  public void testSometimesRequiredFields() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    // Require some of the sometimes required fields.
    newConfigMap.put("extMetadataType", "lob");
    // Remove the sometimes required fields.
    for (String field : DBConnectorType.SOMETIMES_REQUIRED_FIELDS) {
      newConfigMap.put(field, "");
    }
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  public void testMissingBlobClob() {
    testSometimesRequiredField("lob", "lobField", "fetchURLField", "fetchURL");
  }

  public void testMissingDocumentId() {
    testSometimesRequiredField("url", "documentIdField",
        "baseURL", "http://myhost/app/");
  }

  public void testMissingBaseUrl() {
    testSometimesRequiredField("url", "baseURL", "documentIdField", "fname");
  }

  /**
   * Test sometimes required fields that are required when other
   * fields are set.
   */
  private void testSometimesRequiredField(String extMetadataType,
      String missing, String present, String value) {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put("extMetadataType", extMetadataType);
    newConfigMap.put(missing, "");
    newConfigMap.put(present, value);

    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertNotNull(configRes);
    String message = configRes.getMessage();
    assertTrue(message,
        message.contains(BUNDLE.getString("MISSING_ATTRIBUTES")));
    String label = BUNDLE.getString(missing);
    int index = message.indexOf(label);
    assertFalse(message + " does not contain " + label, index == -1);
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

  private static final String[] SINGLE_FIELD_NAMES = {
    "fname", "fNAME", "FNAME", " FNAME", "FNAME " };

  private static final String ACTUAL_FIELD_NAME = "FNAME";

  private static final String[] MULTIPLE_FIELD_NAMES = {
    "id,fname", "iD,fNAME", "ID,fname", "id,FNAME", // cases
    " ID,FNAME", "ID ,FNAME", "ID, FNAME", "ID,FNAME ", // whitespace
    " id,fname", "iD ,fNAME", "Id, Fname", // mixed-case whitespace
    "ID,fNAME", "iD,FNAME" }; // one exact match, one mixed-case

  private static final String INVALID_FIELD_NAME = "not_a_field";

  /**
   * Tests different cases and extra whitespace in field names. All of
   * the errors are collected before reporting the failures.
   *
   * @param properties property names and the values required for the
   *     property name under test
   * @param propertyName the property name under test
   * @param propertyValues the property values (database field names) to test
   * @param expectedValue the field name returned by the database
   * @param bundleMessage the expected error message key for invalid
   *     column names
   */
  private void testFieldNames(Map<String, String> properties,
      String propertyName, String[] propertyValues, String expectedValue,
      String bundleMessage) {
    StringBuilder errors = new StringBuilder();
    for (String propertyValue : propertyValues) {
      ConfigureResponse configRes =
          validateConfig(properties, propertyName, propertyValue);
      if (configRes.getMessage() != null
          || configRes.getFormSnippet() != null) {
        errors.append(propertyName + "=" + propertyValue + ": "
            + configRes.getMessage() + "\n");
      } else if (expectedValue != null
          && !configRes.getConfigData().get(propertyName).equals(
              expectedValue)) {
        errors.append(propertyName + "=" + propertyValue + ": expected:<" +
            expectedValue + "> but was:<"
            + configRes.getConfigData().get(propertyName) + ">\n");
      }
    }

    ConfigureResponse configRes =
        validateConfig(properties, propertyName, INVALID_FIELD_NAME);
    if (configRes.getMessage() == null) {
      errors.append(propertyName + "=" + INVALID_FIELD_NAME
          + ": Unexpected null\n");
    } else if (!configRes.getMessage().equals(
        BUNDLE.getString(bundleMessage))) {
      errors.append(propertyName + "=" + INVALID_FIELD_NAME + ": "
          + configRes.getMessage() + "\n");
    }

    if (errors.length() > 0) {
      fail(errors.toString());
    }
  }

  /**
   * Helper method to call validateConfig with the fixture values and
   * the given properties added to the configuration map.
   */
  private ConfigureResponse validateConfig(Map<String, String> properties,
      String propertyName, String propertyValue) {
    Map<String, String> newConfigMap = Maps.newHashMap();
    newConfigMap.putAll(configMap);
    newConfigMap.putAll(properties);
    newConfigMap.put(propertyName, propertyValue);
    ConfigureResponse response = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);

    // We need to pass the map we created, which was modified by
    // validateConfig, back to the caller.
    if (response == null) {
      return new ConfigureResponse(null, null, newConfigMap);
    } else {
      return response;
    }
  }

  public void testPrimaryKeySingleValues() {
    testFieldNames(ImmutableMap.<String, String>of(), "primaryKeysString",
        SINGLE_FIELD_NAMES, null, "TEST_PRIMARY_KEYS");
  }

  public void testPrimaryKeyMultipleValues() {
    testFieldNames(ImmutableMap.<String, String>of(), "primaryKeysString",
        MULTIPLE_FIELD_NAMES, null, "TEST_PRIMARY_KEYS");
  }

  public void testLastModifiedDateValues() {
    testFieldNames(ImmutableMap.<String, String>of(), "lastModifiedDate",
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "INVALID_COLUMN_NAME");
  }

  public void testLobFieldValues() {
    testFieldNames(ImmutableMap.of("extMetadataType", "lob"), "lobField",
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "INVALID_COLUMN_NAME");
  }

  public void testFetchUrlFieldValues() {
    testFieldNames(
        ImmutableMap.of("extMetadataType", "lob", "lobField", "LNAME"),
        "fetchURLField", SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME,
        "INVALID_COLUMN_NAME");
  }

  public void testDocumentURLFieldValues() {
    testFieldNames(ImmutableMap.of("extMetadataType", "url"),
        "documentURLField", SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME,
        "INVALID_COLUMN_NAME");
  }

  public void testDocumentIdFieldValues() {
    testFieldNames(ImmutableMap.of("extMetadataType", "docId",
        "baseURL", "http://example.com/"), "documentIdField",
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "INVALID_COLUMN_NAME");
  }

  private void testOneFieldWithoutAnother(String oneField, String oneValue,
      String missingField) {
    ConfigureResponse response = validateConfig(
        ImmutableMap.of("extMetadataType", "docId"), oneField, oneValue);
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage(), response.getMessage().startsWith(
        BUNDLE.getString("MISSING_ATTRIBUTES")));
    assertTrue(response.getMessage(), response.getMessage().endsWith(
        BUNDLE.getString(missingField)));
  }

  public void testDocumentIdFieldWithoutBaseUrl() {
    testOneFieldWithoutAnother("documentIdField", "fname", "baseURL");
  }

  public void testBaseUrlWithoutDocumentIdField() {
    testOneFieldWithoutAnother("baseURL", "http://example.com/",
        "documentIdField");
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
