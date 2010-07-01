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

import com.google.enterprise.connector.spi.AuthenticationIdentity;
import com.google.enterprise.connector.spi.AuthorizationResponse;
import com.google.enterprise.connector.spi.RepositoryException;

import java.util.ArrayList;
import java.util.Collection;

public class DBConnectorAuthorizationManagerTest extends DBTestBase {
	private DBConnectorAuthorizationManager authZmanager = null;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		/*
		 * Create table that map username and permitted docIds.
		 */
		runDBScript(CREATE_USER_DOC_MAP_TABLE);
		/*
		 * Load test data.
		 */
		runDBScript(LOAD_USER_DOC_MAP_TEST_DATA);

		DBTraversalManager traversalManager = getDBTraversalManager();
		DBClient dbClient = traversalManager.getDbClient();
		authZmanager = new DBConnectorAuthorizationManager(dbClient);
	}

	/**
	 * Test method authorizeDocdds
	 */
	public void testAuthorizeDocids() {

		/*
		 * Create AuthenticationIdentity for user-name "user1"
		 */
		AuthenticationIdentity authNIdentity = new AuthenticationIdentity() {

			private String userName = "user1";

			public String getUsername() {
				return userName;
			}

			public String getPassword() {
				return null;
			}

			public String getDomain() {
				return null;
			}
		};

		Collection<String> docIds = new ArrayList<String>();

		// build doc Ids for testing
		String docId1 = DocIdUtil.getBase64EncodedString("1");
		String docId2 = DocIdUtil.getBase64EncodedString("2");
		String docId3 = DocIdUtil.getBase64EncodedString("3");
		String docId4 = DocIdUtil.getBase64EncodedString("4");

		// add doc Ids in the collection of documents to be authorized.
		docIds.add(docId1);
		docIds.add(docId2);
		docIds.add(docId3);
		docIds.add(docId4);

		try {
			/*
			 * authorized above collection of doc Ids for user-name "user1"
			 */

			Collection<AuthorizationResponse> authZResponce = authZmanager.authorizeDocids(docIds, authNIdentity);

			assertNotNull(authZResponce);

			/*
			 * Iterate over collection of AuthorizationResponse and check for
			 * PERMIT and DENY documents. In test data, user-name "user1" is
			 * allowed to view documents with docId 'docId1' and 'docId3' and
			 * denied for 'docId2' and 'docId4'.
			 */
			for (AuthorizationResponse responce : authZResponce) {
				String docId = responce.getDocid();

				if (docId1.equals(docId)) {
					assertTrue(responce.isValid());
				}

				if (docId2.equals(docId)) {
					assertFalse(responce.isValid());
				}
				if (docId3.equals(docId)) {
					assertTrue(responce.isValid());
				}
				if (docId4.equals(docId)) {
					assertFalse(responce.isValid());
				}
			}
		} catch (RepositoryException e) {
			fail("Exception occured while authorization");
		}
	}

	@Override
	protected void tearDown() throws Exception {
		// drop database table under test
		runDBScript(DROP_USER_DOC_MAP_TABLE);
	}

}
