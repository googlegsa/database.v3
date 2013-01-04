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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
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

  private DBConnectorType connectorType;

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
  }

  /**
   * Test method for validateConfig method.
   */
  public void testValidateConfig() {
    LOG.info("Testing validateConfig()...");
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    // Remove a required field.
    newConfigMap.put("dbName", "");
    MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
        TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);

    LOG.info("Checking for Required field Database Name...");
    String strPattern = ".*Required fields are missing.*";
    Pattern pattern = Pattern.compile(strPattern);
    Matcher match = pattern.matcher(configRes.getMessage());
    assertTrue(match.find());

    LOG.info("Checking when all required fields are provided...");
    configRes = this.connectorType.validateConfig(configMap,
        Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
    LOG.info("[ validateConfig() ] Test Passed.");
  }

  /**
   * Scenario when more than one primary key is provided and connector is
   * configured for using the parameterized crawl query.
   */
  public void testParameterizedQueryAndMutiplePrimaryKeys() {
    MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
        TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put("sqlQuery",
                     "select * from TestEmpTable where id > #{value}");
    newConfigMap.put("primaryKeysString", "id,fname");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    assertEquals("Single Primary key should be used when configuring a "
                 + "parameterized crawl query", configRes.getMessage());
  }

  /**
   * Scenario when single primary key is provided and connector is configured
   * for using the parameterized crawl query.
   */
  public void testParameterizedQueryAndSinglePrimaryKeys() {
    MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
        TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);
    Map<String, String> newConfigMap = Maps.newHashMap(this.configMap);
    newConfigMap.put("sqlQuery", "select * from TestEmpTable where id > #{value}");
    ConfigureResponse configRes = this.connectorType.validateConfig(
        newConfigMap, Locale.ENGLISH, mdbConnectorFactory);
    if (configRes != null) {
      fail(configRes.getMessage());
    }
  }

  /**
   * Test for getConfigForm method.
   */
  public void testGetConfigForm() {
    final ConfigureResponse configureResponse =
        connectorType.getConfigForm(Locale.ENGLISH);
    final String configForm = configureResponse.getFormSnippet();
    boolean check = checkForExpectedFields(configForm);
    assertTrue(check);
  }

  /**
   * Test for getPopulatedConfigForm method.
   */
  public void testGetPopulatedConfigForm() {
    final ConfigureResponse configureResponse =
        connectorType.getPopulatedConfigForm(configMap, Locale.ENGLISH);
    final String configForm = configureResponse.getFormSnippet();
    boolean check = checkForExpectedFields(configForm);
    assertTrue(check);
  }

  /**
   * Tests expected patterns in html text of configuration form.
   *
   * @param configForm is html string of configuration form for database
   *        connector
   */
  private boolean checkForExpectedFields(final String configForm) {
    LOG.info("Checking for Sql Query field...");
    String strPattern = "<textarea .*name=\"sqlQuery\".*>";
    Pattern pattern = Pattern.compile(strPattern);
    Matcher match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Driver Class Name field...");
    strPattern = "<input.*size=\"40\" name=\"driverClassName\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Password field...");
    strPattern = "<input.*size=\"40\" name=\"password\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Primary Keys String field...");
    strPattern = "<input.*size=\"40\" name=\"primaryKeysString\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for login field...");
    strPattern = "<input.*size=\"40\" name=\"login\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Database Name field...");
    strPattern = "<input.*size=\"40\" name=\"dbName\".*>";
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
    strPattern = "<input.*size=\"40\" name=\"hostname\".*>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Connection URL field...");
    strPattern = "<input.*size=\"40\" name=\"connectionUrl\".*>";
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
    strPattern = "<input.*size=\"40\".*name=\"lastModifiedDate\".*"
        + "id=\"lastModifiedDate\".*/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    LOG.info("Checking for Document Ttitle Field...");
    strPattern = "<input.*size=\"40\" name=\"lastModifiedDate\".*"
        + "id=\"lastModifiedDate\".*/>";
    pattern = Pattern.compile(strPattern);
    match = pattern.matcher(configForm);
    assertTrue(match.find());

    return true;
  }
}
