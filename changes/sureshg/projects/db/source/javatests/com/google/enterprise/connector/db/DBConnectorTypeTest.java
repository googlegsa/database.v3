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

import com.google.enterprise.connector.spi.ConfigureResponse;

public class DBConnectorTypeTest extends TestCase {

	private static final Logger LOG = Logger.getLogger(DBConnectorTypeTest.class.getName());

	private DBConnectorType connectorType;
	private Set<String> configKeys;
	private String[] keys = new String[] { "login", "password",
			"connectionUrl", "dbName", "hostname", "driverClassName",
			"sqlQuery", "primaryKeysString", "xslt" };

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
		configMap.put("login", "root");
		configMap.put("driverClassName", "com.mysql.jdbc.Driver");
		configMap.put("password", "root");
		configMap.put("primaryKeysString", "id");
		configMap.put("connectionUrl", "jdbc:mysql://ps4210:3306/MySQL");
		configMap.put("sqlQuery", "SELECT * FROM myemp");
		configMap.put("dbName", "");
		configMap.put("hostname", "PS4210.persistent");
		configMap.put("xslt", "");
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
		configMap.put("dbName", "MySQL");
		configRes = this.connectorType.validateConfig(this.configMap, Locale.ENGLISH, mdbConnectorFactory);
		assertNull(configRes);
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

		return true;
	}
}
