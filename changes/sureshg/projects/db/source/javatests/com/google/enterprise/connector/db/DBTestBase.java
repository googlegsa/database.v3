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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.Session;
import com.ibatis.common.jdbc.ScriptRunner;
import com.ibatis.common.resources.Resources;

public class DBTestBase extends TestCase {

	private Map<String, String> configMap = new HashMap<String, String>();
	private GlobalState globalState;
	private static final Logger LOG = Logger.getLogger(DBTestBase.class.getName());

	public GlobalState getGlobalState() {
		return globalState;
	}

	public static final String CREATE_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/createTable.sql";
	public static final String LOAD_TEST_DATA = "com/google/enterprise/connector/db/config/loadTestData.sql";
	public static final String TRUNCATE_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/truncateTestTable.sql";
	public static final String DROP_TEST_DB_TABLE = "com/google/enterprise/connector/db/config/dropdb.sql";

	@Override
	protected void setUp() throws Exception {
		TestDirectoryManager testDirManager = new TestDirectoryManager(this);
		configMap.put("login", "root");
		configMap.put("password", "root");
		configMap.put("connectionUrl", "jdbc:mysql://10.88.45.40:3306/MySQL");
		configMap.put("dbName", "MySQL");
		configMap.put("hostname", "persistent.PS4210");
		configMap.put("driverClassName", "com.mysql.jdbc.Driver");
		configMap.put("sqlQuery", "SELECT * FROM TestEmpTable");
		configMap.put("primaryKeysString", "id");
		configMap.put("googleConnectorWorkDir", testDirManager.getTmpDir());
		configMap.put("xslt", "");
		globalState = new GlobalState(testDirManager.getTmpDir());
		runDBScript(CREATE_TEST_DB_TABLE);
	}

	public DBConnector getConnector() {
		MockDBConnectorFactory mdbConnectorFactory = new MockDBConnectorFactory(
				TestUtils.TESTCONFIG_DIR + TestUtils.CONNECTOR_INSTANCE_XML);

		DBConnector connector = (DBConnector) mdbConnectorFactory.makeConnector(configMap);
		return connector;
	}

	public Session getSession() throws RepositoryException {

		return getConnector().login();
	}

	public DBTraversalManager getDBTraversalManager()
			throws RepositoryException {
		return (DBTraversalManager) getSession().getTraversalManager();
	}

	@Override
	protected void tearDown() throws Exception {
		runDBScript(DROP_TEST_DB_TABLE);
		super.tearDown();
	}

	/*
	 * This method will set up initial state of GlobalState class. Test
	 * documents are added toGlobalState for testing
	 */
	public void setUpInitialState() {
		DateTime queryExecutionTime = new DateTime();
		globalState.setQueryExecutionTime(queryExecutionTime);
		try {
			for (Map<String, Object> row : TestUtils.getDBRows()) {
				DBDocument dbDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), row, "localhost", null);
				globalState.addDocument(dbDoc);
			}
		} catch (DBException dbe) {
			LOG.info("DBException is occured while closing database connection"
					+ dbe.toString());
		}
	}

	/**
	 * This method executes the database script.
	 * 
	 * @param scriptPath path of SQL script file
	 */
	public void runDBScript(String scriptPath) {
		Connection connection = null;
		try {
			connection = getDBTraversalManager().getDbClient().getSqlMapClient().getDataSource().getConnection();
			ScriptRunner runner = new ScriptRunner(connection, false, true);
			runner.runScript(Resources.getResourceAsReader(scriptPath));
		} catch (SQLException se) {
			LOG.info("SQLException is occured while closing database connection"
					+ se.toString());
		} catch (RepositoryException re) {
			LOG.info("RepositoryException is occured while closing database connection"
					+ re.toString());
		} catch (IOException ioe) {
			LOG.info("IOException is occured while closing database connection"
					+ ioe.toString());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException se) {
					LOG.info("SQLException is occured while closing database connection"
							+ se.toString());
				}
			}
		}
	}

}
