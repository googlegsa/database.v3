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

import static com.google.enterprise.connector.db.DBConnectorType.AUTHZ_QUERY;
import static com.google.enterprise.connector.db.DBConnectorType.BASE_URL;
import static com.google.enterprise.connector.db.DBConnectorType.BLOB_CLOB;
import static com.google.enterprise.connector.db.DBConnectorType.CLOB_BLOB_FIELD;
import static com.google.enterprise.connector.db.DBConnectorType.COMPLETE_URL;
import static com.google.enterprise.connector.db.DBConnectorType.CONNECTION_URL;
import static com.google.enterprise.connector.db.DBConnectorType.DOCUMENT_ID_FIELD;
import static com.google.enterprise.connector.db.DBConnectorType.DOCUMENT_URL_FIELD;
import static com.google.enterprise.connector.db.DBConnectorType.DOC_ID;
import static com.google.enterprise.connector.db.DBConnectorType.DRIVER_CLASS_NAME;
import static com.google.enterprise.connector.db.DBConnectorType.EXT_METADATA_TYPE;
import static com.google.enterprise.connector.db.DBConnectorType.EXT_METADATA_TYPE_FIELDS;
import static com.google.enterprise.connector.db.DBConnectorType.EXT_METADATA_TYPE_ONCLICKS;
import static com.google.enterprise.connector.db.DBConnectorType.FETCH_URL_FIELD;
import static com.google.enterprise.connector.db.DBConnectorType.LAST_MODIFIED_DATE_FIELD;
import static com.google.enterprise.connector.db.DBConnectorType.LOGIN;
import static com.google.enterprise.connector.db.DBConnectorType.NO_EXT_METADATA;
import static com.google.enterprise.connector.db.DBConnectorType.PASSWORD;
import static com.google.enterprise.connector.db.DBConnectorType.PRIMARY_KEYS_STRING;
import static com.google.enterprise.connector.db.DBConnectorType.SQL_QUERY;
import static com.google.enterprise.connector.db.DBConnectorType.TEXT;
import static com.google.enterprise.connector.db.DBConnectorType.XSLT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.enterprise.connector.spi.ConfigureResponse;
import com.google.enterprise.connector.util.XmlParseUtil;

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
    configMap.put(AUTHZ_QUERY, "");
    configMap.put(BASE_URL, "");
    configMap.put(DOCUMENT_ID_FIELD, "");
    configMap.put(DOCUMENT_URL_FIELD, "");
    configMap.put(FETCH_URL_FIELD, "");
    configMap.put(CLOB_BLOB_FIELD, "");

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
   * Disabled fields are not submitted, so they won't appear in the
   * map at all.
   */
  public void testSometimesDisabledFields() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put(EXT_METADATA_TYPE, NO_EXT_METADATA);
    // Remove the sometimes disabled fields.
    for (String field : DBConnectorType.SOMETIMES_DISABLED_FIELDS) {
      newConfigMap.remove(field);
    }
    ConfigureResponse configRes = connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  public void testMissingBlobClob() {
    String formSnippet =
        testSometimesRequiredField(BLOB_CLOB, CLOB_BLOB_FIELD, "", "");
    assertExpectedFields(formSnippet, BLOB_CLOB);
  }

  public void testMissingBlobClobWithFetchUrl() {
    String formSnippet = testSometimesRequiredField(BLOB_CLOB, CLOB_BLOB_FIELD,
        FETCH_URL_FIELD, "fetchURL");
    assertExpectedFields(formSnippet, BLOB_CLOB);
  }

  public void testMissingDocumentUrl() {
    String formSnippet =
        testSometimesRequiredField(COMPLETE_URL, DOCUMENT_URL_FIELD, "", "");
    assertExpectedFields(formSnippet, COMPLETE_URL);
  }

  public void testMissingDocumentId() {
    String formSnippet = testSometimesRequiredField(DOC_ID, DOCUMENT_ID_FIELD,
        BASE_URL, "http://myhost/app/");
    assertExpectedFields(formSnippet, DOC_ID);
  }

  public void testMissingBaseUrl() {
    String formSnippet = testSometimesRequiredField(DOC_ID, BASE_URL,
        DOCUMENT_ID_FIELD, "fname");
    assertExpectedFields(formSnippet, DOC_ID);
  }

  /**
   * Test sometimes required fields that are required when other
   * fields are set.
   */
  private String testSometimesRequiredField(String extMetadataType,
      String missing, String present, String value) {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put(EXT_METADATA_TYPE, extMetadataType);
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
    return configRes.getFormSnippet();
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
    newConfigMap.put(SQL_QUERY, "update TestEmpTable set dept = 42");
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
    newConfigMap.put(SQL_QUERY, "choose the red pill");
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
    newConfigMap.put(SQL_QUERY,
                     "select * from TestEmpTable where id > #{value}");
    newConfigMap.put(PRIMARY_KEYS_STRING, "id,fname");
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

  private static final String ACTUAL_FIELD_NAMES = "ID,FNAME";

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
    testFieldNames(ImmutableMap.<String, String>of(), PRIMARY_KEYS_STRING,
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "TEST_PRIMARY_KEYS");
  }

  public void testPrimaryKeyMultipleValues() {
    testFieldNames(ImmutableMap.<String, String>of(), PRIMARY_KEYS_STRING,
        MULTIPLE_FIELD_NAMES, ACTUAL_FIELD_NAMES, "TEST_PRIMARY_KEYS");
  }

  public void testPrimaryKeyEmptyValues() {
    StringBuilder errors = new StringBuilder();
    for (String empty : new String[] { ",", "   ", "   , ,," }) {
      ConfigureResponse configRes = validateConfig(
          ImmutableMap.<String, String>of(), PRIMARY_KEYS_STRING, empty);
      if (configRes.getMessage() == null) {
        errors.append("primaryKeysString=" + empty
            + ": Unexpected null\n");
      } else if (!configRes.getMessage().equals(
              BUNDLE.getString("TEST_PRIMARY_KEYS"))) {
        errors.append("primaryKeysString=" + empty + ": "
            + configRes.getMessage() + "\n");
      }
    }

    if (errors.length() > 0) {
      fail(errors.toString());
    }
  }

  public void testLastModifiedDateValues() {
    testFieldNames(ImmutableMap.<String, String>of(), LAST_MODIFIED_DATE_FIELD,
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "INVALID_COLUMN_NAME");
  }

  public void testLobFieldValues() {
    testFieldNames(ImmutableMap.of(EXT_METADATA_TYPE, BLOB_CLOB),
        CLOB_BLOB_FIELD, SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME,
        "INVALID_COLUMN_NAME");
  }

  public void testFetchUrlFieldValues() {
    testFieldNames(
        ImmutableMap.of(EXT_METADATA_TYPE, BLOB_CLOB, CLOB_BLOB_FIELD, "LNAME"),
        FETCH_URL_FIELD, SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME,
        "INVALID_COLUMN_NAME");
  }

  public void testDocumentURLFieldValues() {
    testFieldNames(ImmutableMap.of(EXT_METADATA_TYPE, COMPLETE_URL),
        DOCUMENT_URL_FIELD, SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME,
        "INVALID_COLUMN_NAME");
  }

  public void testDocumentIdFieldValues() {
    testFieldNames(ImmutableMap.of(EXT_METADATA_TYPE, DOC_ID,
        BASE_URL, "http://example.com/"), DOCUMENT_ID_FIELD,
        SINGLE_FIELD_NAMES, ACTUAL_FIELD_NAME, "INVALID_COLUMN_NAME");
  }

  private void testOneFieldWithoutAnother(String oneField, String oneValue,
      String missingField) {
    ConfigureResponse response = validateConfig(
        ImmutableMap.of(EXT_METADATA_TYPE, DOC_ID), oneField, oneValue);
    assertNotNull(response.getMessage());
    assertTrue(response.getMessage(), response.getMessage().startsWith(
        BUNDLE.getString("MISSING_ATTRIBUTES")));
    assertTrue(response.getMessage(), response.getMessage().endsWith(
        BUNDLE.getString(missingField)));
  }

  public void testDocumentIdFieldWithoutBaseUrl() {
    testOneFieldWithoutAnother(DOCUMENT_ID_FIELD, "fname", BASE_URL);
  }

  public void testBaseUrlWithoutDocumentIdField() {
    testOneFieldWithoutAnother(BASE_URL, "http://example.com/",
        DOCUMENT_ID_FIELD);
  }

  /**
   * Scenario when single primary key is provided and connector is configured
   * for using the parameterized crawl query.
   */
  public void testParameterizedQueryAndSinglePrimaryKeys() {
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put(SQL_QUERY,
        "select * from TestEmpTable where id > #{value}");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  /** Tests an authZ query with no #{username} placeholder. */
  public void testInvalidAuthZQuery() throws Exception {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    newConfigMap.put(AUTHZ_QUERY, "choose the red pill");
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
    newConfigMap.put(AUTHZ_QUERY, "select id from TestEmpTable where "
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
    newConfigMap.put(AUTHZ_QUERY, "update TestEmpTable set dept = 42"
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
    assertExpectedFields(configForm, NO_EXT_METADATA);
  }

  /** Test for getPopulatedConfigForm.
   *
   * @param expected the expected pattern for the extMetadataType property;
   *     multiple values may be selected (it's a bug)
   * @param configured the configured value of the extMetadataType property
   * @param propertyName another property name to configure
   * @param propertyValue the value to give that other property
   */
  private void testGetPopulatedConfigForm(String expected, String configured,
      String propertyName, String propertyValue) {
    Map<String, String> newConfigMap = Maps.newHashMap(configMap);
    newConfigMap.put(EXT_METADATA_TYPE, configured);
    newConfigMap.put(propertyName, propertyValue);
    ConfigureResponse configureResponse =
        connectorType.getPopulatedConfigForm(newConfigMap, Locale.ENGLISH);
    String configForm = configureResponse.getFormSnippet();
    assertExpectedFields(configForm, expected);
  }

  public void testGetPopulatedConfigForm_empty() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "", XSLT, "");
  }

  public void testGetPopulatedConfigForm_empty_lob() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "",
        CLOB_BLOB_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_empty_fetchUrl() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "",
        FETCH_URL_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_empty_url() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "",
        DOCUMENT_URL_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_empty_docId() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "",
        DOCUMENT_ID_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_empty_baseUrl() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, "",
        BASE_URL, "ignored");
  }

  public void testGetPopulatedConfigForm_noExt() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, NO_EXT_METADATA, XSLT, "");
  }

  public void testGetPopulatedConfigForm_lob() {
    testGetPopulatedConfigForm(BLOB_CLOB, BLOB_CLOB,
        CLOB_BLOB_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_lob_empty() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, BLOB_CLOB, CLOB_BLOB_FIELD, "");
  }

  public void testGetPopulatedConfigForm_lob_fetchUrl() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, BLOB_CLOB,
        FETCH_URL_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_url() {
    testGetPopulatedConfigForm(COMPLETE_URL, COMPLETE_URL,
        DOCUMENT_URL_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_url_empty() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, COMPLETE_URL,
        DOCUMENT_URL_FIELD, "");
  }

  public void testGetPopulatedConfigForm_docId() {
    testGetPopulatedConfigForm(DOC_ID, DOC_ID, DOCUMENT_ID_FIELD, "ignored");
  }

  public void testGetPopulatedConfigForm_docId_empty() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, DOC_ID, DOCUMENT_ID_FIELD, "");
  }

  public void testGetPopulatedConfigForm_docId_baseUrl() {
    testGetPopulatedConfigForm(NO_EXT_METADATA, DOC_ID, BASE_URL, "ignored");
  }

  /** Tests an empty config form for valid XHTML. */
  public void testXhtmlGetConfigForm() throws Exception {
    ConfigureResponse configureResponse =
        connectorType.getConfigForm(Locale.ENGLISH);
    XmlParseUtil.validateXhtml(configureResponse.getFormSnippet());
  }

  /** Tests a populated config form for valid XHTML. */
  public void testXhtmlGetPopulatedConfigForm() throws Exception {
    ConfigureResponse configureResponse =
        connectorType.getPopulatedConfigForm(configMap, Locale.ENGLISH);
    XmlParseUtil.validateXhtml(configureResponse.getFormSnippet());
  }

  /**
   * Tests expected patterns in html text of configuration form.
   *
   * @param configForm is html string of configuration form for database
   *        connector
   * @param extMetadata the expected pattern of the extMetadataType radio button
   */
  private void assertExpectedFields(String configForm, String extMetadata) {
    assertContainsTextArea(configForm, SQL_QUERY);
    assertContainsInput(configForm, TEXT, DRIVER_CLASS_NAME);
    assertContainsInput(configForm, PASSWORD, PASSWORD);
    assertContainsInput(configForm, TEXT, PRIMARY_KEYS_STRING);
    assertContainsInput(configForm, TEXT, LOGIN);
    assertContainsTextArea(configForm, AUTHZ_QUERY);
    assertContainsInput(configForm, TEXT, CONNECTION_URL);
    assertContainsRadio(configForm, EXT_METADATA_TYPE, NO_EXT_METADATA,
        extMetadata);
    assertContainsTextArea(configForm, XSLT, extMetadata);
    assertContainsRadio(configForm, EXT_METADATA_TYPE, COMPLETE_URL,
        extMetadata);
    assertContainsInput(configForm, TEXT, DOCUMENT_URL_FIELD, extMetadata);
    assertContainsRadio(configForm, EXT_METADATA_TYPE, DOC_ID, extMetadata);
    assertContainsInput(configForm, TEXT, DOCUMENT_ID_FIELD, extMetadata);
    assertContainsInput(configForm, TEXT, BASE_URL, extMetadata);
    assertContainsRadio(configForm, EXT_METADATA_TYPE, BLOB_CLOB, extMetadata);
    assertContainsInput(configForm, TEXT, CLOB_BLOB_FIELD, extMetadata);
    assertContainsInput(configForm, TEXT, FETCH_URL_FIELD, extMetadata);
    assertContainsInput(configForm, TEXT, LAST_MODIFIED_DATE_FIELD);
  }

  /** Asserts that the given enabled textarea is present in the form. */
  private void assertContainsTextArea(String configForm, String name) {
    assertContainsTextArea(configForm, name, false);
  }

  /**
   * Asserts that the given textarea is present in the form, and that
   * it is enabled when the selected extMetadataType radio button
   * value matches the one governing this input.
   */
  private void assertContainsTextArea(String configForm, String name,
      String extMetadataType) {
    assertContainsTextArea(configForm, name,
        !extMetadataType.equals(EXT_METADATA_TYPE_FIELDS.get(name)));
  }

  private void assertContainsTextArea(String configForm, String name,
      boolean disabled) {
    assertContains(configForm, String.format(
        "<textarea rows=\"10\" cols=\"50\" name=\"%s\" id=\"%s\"%s>",
        name, name, (disabled) ? " disabled=\"disabled\"" : ""));
  }

  /** Asserts that the given enabled input is present in the form. */
  private void assertContainsInput(String configForm, String type,
      String name) {
    assertContainsInput(configForm, type, name, false);
  }

  /**
   * Asserts that the given input is present in the form, and that
   * it is enabled when the selected extMetadataType radio button
   * value matches the one governing this input.
   */
  private void assertContainsInput(String configForm, String type,
      String name, String extMetadataType) {
    assertContainsInput(configForm, type, name,
        !extMetadataType.equals(EXT_METADATA_TYPE_FIELDS.get(name)));
  }

  /** Helper method for the other two overloads. */
  private void assertContainsInput(String configForm, String type,
      String name, boolean disabled) {
    assertContains(configForm, String.format("<input type=\"%s\" "
        + "size=\"50\" name=\"%s\" id=\"%s\"( value=\".*\")?%s/>",
            type, name, name, (disabled) ? " disabled=\"disabled\"" : ""));
  }

  private void assertContainsRadio(String configForm, String name,
      String value, String extMetadata) {
    boolean selected = value.matches(extMetadata);
    assertContains(configForm, String.format("<input type=\"radio\" "
        + "name=\"%s\" value=\"%s\" id=\"%s_%s\"%s onclick=\"%s\"/>",
        name, value, name, value, (selected) ? " checked=\"checked\"" : "",
        Pattern.quote(EXT_METADATA_TYPE_ONCLICKS.get(value))));
  }

  private void assertContains(String configForm, String pattern) {
    assertTrue("Unable to find " + pattern + " in " + configForm,
        Pattern.compile(pattern).matcher(configForm).find());
  }
}
