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

import java.util.Map;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import com.google.enterprise.connector.spi.RepositoryException;

public class DBDocumentListTest extends TestCase {
	private static final Logger LOG = Logger.getLogger(DBDocumentListTest.class.getName());
	private DBDocumentList docList;
	GlobalState globalState;
	DateTime dt;

	@Override
	protected void setUp() throws Exception {
		TestDirectoryManager testDirManager = new TestDirectoryManager(this);
		globalState = new GlobalState(testDirManager.getTmpDir());
		docList = new DBDocumentList(globalState);
		for (Map<String, Object> row : TestUtils.getDBRows()) {
			DBDocument dbDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), row, "localhost", null);
			docList.addDocument(dbDoc);
		}
		dt = new DateTime();
		globalState.setQueryExecutionTime(dt);
	}

	public final void testCheckpoint() throws RepositoryException {
		String checkpointStr = docList.checkpoint();
		LOG.info(checkpointStr);
		assertTrue(checkpointStr.contains(dt.toString()));
		assertTrue(checkpointStr.contains("6fd5643953e6e60188c93b89c71bc1808eb7edc2"));
	}

	public final void testNextDocument() {
		docList.nextDocument();
		assertEquals(1, globalState.getDocsInFlight().size());
		assertEquals(3, docList.size());
	}
}
