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



import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.spi.RepositoryException;

import java.sql.Connection;
import java.sql.SQLException;




public class DBClientTest extends DBTestBase {


	/* @Override */
	protected void setUp() throws Exception {
		super.setUp();
		runDBScript(CREATE_TEST_DB_TABLE);
		runDBScript(LOAD_TEST_DATA);
	}

    public void testDBClient() {

        try {
			DBClient dbClient = getDbClient();
			assertNotNull(dbClient);
		} catch (RepositoryException e) {
			fail("Repository Exception in testDBClient");

        }

    }

	/* @Override */
	protected void tearDown() throws Exception {
		super.tearDown();
	}


    public void testConnectivity() {

        Connection connection;
		try {
			connection = getDbClient().getSqlMapClient().getDataSource().getConnection();
			assertNotNull(connection);
		} catch (SQLException e) {
			fail("SQL Exception in testConnectivity");
		} catch (RepositoryException e) {
			fail("Repository Exception in testConnectivity");
		}

    }


}
