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

import com.google.enterprise.connector.spi.RepositoryException;

import java.util.Date;

import junit.framework.Assert;

/**
 * This is JUnit test case for DBTraversalManager.
 * 
 * @author Suresh_Ghuge
 */
public class DBTraversalManagerTest extends DBTestBase {
	private DBTraversalManager traversalMgr;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		runDBScript(CREATE_TEST_DB_TABLE);
		traversalMgr = getDBTraversalManager();

	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/*
	 * Test for startTraversal method. startTravrsal should return null when
	 * database is empty or crawl cycle is finished. This should return
	 * DocumentList when there are more record(s) to crawl
	 */
	public void testStartTraversal() {
		try {
			// Test a scenario for empty database or when DB crawl cycle is
			// over. This should return null DocumentList that indicate database
			// is empty or crawl cycle is over
			runDBScript(TRUNCATE_TEST_DB_TABLE);
			assertNull(traversalMgr.startTraversal());
			// Test a scenario for a database having more records to crawl. This
			// should return DocumentList with documents.
			runDBScript(LOAD_TEST_DATA);
			assertNotNull(traversalMgr.startTraversal().nextDocument());

		} catch (RepositoryException re) {
			fail("Exception occured at startTraversal:" + re.toString());
		}
	}

	/*
	 * Test method for resumeTraversal() method. resumeTraversal should return
	 * null when database is empty or crawl cycle is finished. This should
	 * return DocumentList when there are more record(s) to crawl
	 */
	public void testResumeTraversal() {
		try {
			// Test a scenario for empty database or when DB crawl cycle is
			// over. This should return null DocumentList that indicate database
			// is empty or crawl cycle is over
			runDBScript(TRUNCATE_TEST_DB_TABLE);
			assertNull(traversalMgr.resumeTraversal("(" + new Date().toString()
					+ ")" + "NO_DOCID"));
			// Test a scenario for a database having more records to crawl. This
			// should return DocumentList with documents.
			runDBScript(LOAD_TEST_DATA);
			assertNotNull(traversalMgr.resumeTraversal("("
					+ new Date().toString() + ")" + "NO_DOCID").nextDocument());
		} catch (RepositoryException re) {
			fail("Exception occured at startTraversal:" + re.toString());
		}

	}

	/*
	 * This test is for setBatchHint method.
	 */
	public void testSetBatchHint() {
		// set batch hint value as 100
		int batchHint = 100;
		traversalMgr.setBatchHint(batchHint);
		int actualBatchHint = traversalMgr.getBatchHint();
		Assert.assertEquals(batchHint, actualBatchHint);
	}
}
