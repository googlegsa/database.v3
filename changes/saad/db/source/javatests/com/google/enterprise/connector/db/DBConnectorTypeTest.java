//Copyright 2009 Google Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.google.enterprise.connector.db;

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
 * This is a JUNit test case for DBConnectorType class. 
 */
public class DBConnectorTypeTest extends TestCase {

	
	private static final Logger LOG = Logger.getLogger(DBConnectorTypeTest.class.getName());

	private DBConnectorType connectorType;
	private Set<String> configKeys;
	private String[] keys = new String[] { "login", "password",
			"connectionUrl", "dbName", "hostname", "driverClassName",
			"sqlQuery", "primaryKeysString", "xslt", "authZQuery",
			"lastModifiedDate", "documentTitle", "externalMetadata",
			"externalMetadata", "documentURLField", "documentIdField",
			"baseURL", "lobField", "fetchURLField", "extMetadataType" };

	private Map<String, String> configMap;

	protected void setUp() throws Exception {
		configKeys = new HashSet<String>(Arrays.asList(keys));
		connectorType = new DBConnectorType(configKeys);
		configMap = new HashMap<String, String>();
		loadConfigMap();
	}

	/*
	 * this method loads key and values in configuration map.
	 */

	private void loadConfigMap() {
		configMap.put("login", LanguageResource.getPropertyValue("login"));
		configMap.put("driverClassName", LanguageResource.getPropertyValue("driverClassName"));
		configMap.put("password", LanguageResource.getPropertyValue("password"));
		configMap.put("primaryKeysString", LanguageResource.getPropertyValue("primaryKeysString"));
		configMap.put("connectionUrl", LanguageResource.getPropertyValue("connectionUrl"));
		configMap.put("sqlQuery", LanguageResource.getPropertyValue("sqlQuery"));
		configMap.put("dbName", "");
		configMap.put("hostname", LanguageResource.getPropertyValue("hostname"));
		configMap.put("xslt", "");
		configMap.put("authZQuery", "");
		configMap.put("lastModifiedDate", "");
		configMap.put("documentTitle", "");
		configMap.put("externalMetadata", "");
		configMap.put("documentURLField", "");
		configMap.put("documentIdField", "");
		configMap.put("baseURL", "");
		configMap.put("lobField", "");
		configMap.put("fetchURLField", "");
		configMap.put("extMetadataType", "");
		configMap.put("googleConnectorWorkDir", TestUtils.TESTCONFIG_DIR);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * test method for validateConfig method.
	 */
	public void testValidateConfig() {

		LOG.info("Testing validateConfig()...");

		MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
				TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);
		ConfigureResponse configRes = this.connectorType.validateConfig(this.configMap, Locale.ENGLISH, mdbConnectorFactory);

		LOG.info("Checking for Required field Database Name...");
		String strPattern = ".*Required fields are missing.*";
		Pattern pattern = Pattern.compile(strPattern);
		Matcher match = pattern.matcher(configRes.getMessage());
		assertTrue(match.find());

		LOG.info("Checking when all required fields are provided...");
		configMap.put("dbName", LanguageResource.getPropertyValue("dbName"));
		configRes = this.connectorType.validateConfig(this.configMap, Locale.ENGLISH, mdbConnectorFactory);
		assertNotNull(configRes);
		LOG.info("[ validateConfig() ] Test Passed.");

	}

	/**
	 * test for getConfigForm method.
	 */
	public void testGetConfigForm() {

		final ConfigureResponse configureResponse = connectorType.getConfigForm(Locale.ENGLISH);
		final String configForm = configureResponse.getFormSnippet();
		boolean check = checkForExpectedFields(configForm);
		assertTrue(check);
	}

	/**
	 * test for getPopulatedConfigForm method
	 */
	public void testGetPopulatedConfigForm() {

		final ConfigureResponse configureResponse = connectorType.getPopulatedConfigForm(configMap, Locale.ENGLISH);
		final String configForm = configureResponse.getFormSnippet();
		boolean check = checkForExpectedFields(configForm);
		assertTrue(check);
	}

	/**
	 * This method tests expected patterns in html text of configuration form
	 * 
	 * @param configForm is html string of configuration form for database
	 *            connector
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
		strPattern = "<input.*type='radio'.*name='extMetadataType'.*value='url'.*onClick='.*'/>";
		pattern = Pattern.compile(strPattern);
		match = pattern.matcher(configForm);
		assertTrue(match.find());

		strPattern = "<input type='radio'.*name='extMetadataType'.*value='docId'.*onClick='.*'/>";
		pattern = Pattern.compile(strPattern);
		match = pattern.matcher(configForm);
		assertTrue(match.find());

		strPattern = "<input type='radio'.*name='extMetadataType' value='lob'.*onClick='.*'/>";
		pattern = Pattern.compile(strPattern);
		match = pattern.matcher(configForm);
		assertTrue(match.find());

		LOG.info("Checking for Last Modified date Field...");
		strPattern = "<input.*size=\"40\".*name=\"lastModifiedDate\".*id=\"lastModifiedDate\".*/>";
		pattern = Pattern.compile(strPattern);
		match = pattern.matcher(configForm);
		assertTrue(match.find());

		LOG.info("Checking for Document Ttitle Field...");
		strPattern = "<input.*size=\"40\" name=\"lastModifiedDate\".*id=\"lastModifiedDate\".*/>";
		pattern = Pattern.compile(strPattern);
		match = pattern.matcher(configForm);
		assertTrue(match.find());

		return true;

	}
}
