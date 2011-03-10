// Copyright 2011 Google Inc.
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


import com.google.enterprise.connector.db.diffing.DBContext;
import com.google.enterprise.connector.db.diffing.DBException;
import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.Util;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Utility class for unittests.
 */
public class TestUtils {

	private static final Logger LOG = Logger.getLogger(TestUtils.class.getName());
	public static final String TESTCONFIG_DIR = "config/";
	public static final String TESTCONNECTORCONFIG_DIR = "";
	public static final String CONNECTOR_INSTANCE_XML = "connectorInstance.xml";
	public static final String SQL_DATA_FILE = "connector_test.sql";
	public static final String UPDATE_SQL_DATA_FILE = "update_connector_test.sql";
	public static final String ADD_SQL_DATA_FILE = "add_connector_test.sql";
	public static final String DELETE_SQL_DATA_FILE = "delete_connector_test.sql";
	public static final String DB_TYPE_MYSQL = "mysql";
	public static final String DB_TYPE_ORACLE = "oracle";
	public static final String DB_TYPE_MS_SQL_SERVER = "sqlserver";
	public static final String DB_TYPE_DB2 = "db2";
	public static final String DB_TYPE_SYBASE = "sybase";

    // This class is not instantiable.
	private TestUtils() {
	}

    public static Map<String, Object> getStandardDBRow() {
		return getRow(1, "first_01", "last_01", "01@google.com");
	}

    /**
	 * Creates a Map Object for a row as returned by the executing a query on a
	 * DB.
	 */
	private static Map<String, Object> getRow(int id, String firstName,
			String lastName, String email) {
		final Map<String, Object> rowMap;
		rowMap = new HashMap<String, Object>();
		rowMap.put("id", id);
		rowMap.put("firstName", firstName);
		rowMap.put("lastName", lastName);
		rowMap.put("email", email);
		return rowMap;
	}

    public static String[] getStandardPrimaryKeys() {
		String[] primaryKeys = new String[2];
		primaryKeys[0] = "id";
		primaryKeys[1] = "lastName";
		return primaryKeys;
	}

    public static List<Map<String, Object>> getDBRows() {
		final List<Map<String, Object>> rows;
		rows = new ArrayList<Map<String, Object>>();
		rows.add(getRow(1, "first_01", "last_01", "01@google.com"));
		rows.add(getRow(2, "first_02", "last_02", "02@google.com"));
		rows.add(getRow(3, "first_03", "last_03", "03@google.com"));
		rows.add(getRow(4, "first_04", "last_04", "04@google.com"));
		return rows;
	}

    public static JsonDocument createDBDoc(int id, String firstName,
			String lastName, String email) throws DBException {
		ProductionTraversalContext context = new ProductionTraversalContext();
		JsonDocument jsonDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), getRow(id, firstName, lastName, email), "localhost", null, null, context);
		return jsonDoc;
	}

    public static Map<String, String> getTypeDriverMap() {
		// Keep it in sync with connectorInstance.xml
		Map<String, String> dbTypeDriver = new HashMap<String, String>();
		dbTypeDriver.put(DB_TYPE_MYSQL, "com.mysql.jdbc.Driver");
		dbTypeDriver.put(DB_TYPE_MS_SQL_SERVER, "com.microsoft.sqlserver.jdbc.SQLServerDriver");
		dbTypeDriver.put(DB_TYPE_DB2, "COM.ibm.db2.jdbc.net.DB2Driver");
		dbTypeDriver.put(DB_TYPE_ORACLE, "oracle.jdbc.OracleDriver");
		dbTypeDriver.put(DB_TYPE_SYBASE, "com.sybase.jdbc2.jdbc.SybDriver");
		return dbTypeDriver;
	}

    public static DBContext getDBContext() throws DBException {

        String connectionUrl = "";
		String hostname = "";
		String driverClassName = "";
		String login = "";
		String password = "";
		String dbName = "";
		String lastModifiedDate = null;
		String documentTitle = "title";
		String documentURLField = "docURL";
		String documentIdField = "docId";
		String baseURL = "http://myhost/app/";
		String lobField = "lob";
		String fetchURLField = "fetchURL";
		String extMetadataType = "";

        DBContext dbContext = new DBContext(connectionUrl, hostname,
				driverClassName, login, password, dbName, lastModifiedDate,
				documentTitle, documentURLField, documentIdField, baseURL,
				lobField, fetchURLField, extMetadataType);

        return dbContext;

    }
}
