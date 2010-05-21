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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class DBConnectorTest extends TestCase {
	Map<String, String> configMap = new HashMap<String, String>();

	@Override
	protected void setUp() throws Exception {
		TestDirectoryManager testDirManager = new TestDirectoryManager(this);
		configMap.put("login", "login_value");
		configMap.put("password", "password_value");
		configMap.put("connectionUrl", "connectionUrl_value");
		configMap.put("dbName", "dbName_value");
		configMap.put("hostname", "host_value");
		configMap.put("driverClassName", "driverClassName_value");
		configMap.put("sqlQuery", "sqlQuery_value");
		configMap.put("primaryKeysString", "primaryKeysString_value");
		configMap.put("googleConnectorWorkDir", testDirManager.getSrcDir());
		configMap.put("xslt", "xslt");
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
	}

	public void testMakeConnector() {
		MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
				TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);
		DBConnector connector = (DBConnector) mdbConnectorFactory.makeConnector(configMap);
		assertNotNull(connector);
	}
}
