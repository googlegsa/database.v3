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

import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;

public class UtilTest extends TestCase {
	private static final Logger LOG = Logger.getLogger(UtilTest.class.getName());

	/**
	 * Test for generating the docId.
	 */
	public final void testGenerateDocId() {
		Map<String, Object> rowMap = null;
		String primaryKeys[] = null;

		try {
			// below line should throw an exception
			String docId = Util.generateDocId(primaryKeys, rowMap);
			fail();
		} catch (DBException e1) {
			e1.printStackTrace();
		}

		try {
			rowMap = TestUtils.getStandardDBRow();
			primaryKeys = TestUtils.getStandardPrimaryKeys();
			assertEquals("6fd5643953e6e60188c93b89c71bc1808eb7edc2", Util.generateDocId(primaryKeys, rowMap));
		} catch (DBException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test for converting DB row to DB Doc.
	 */
	public final void testRowToDoc() {
		Map<String, Object> rowMap = TestUtils.getStandardDBRow();
		String[] primaryKeys = TestUtils.getStandardPrimaryKeys();
		try {
			DBDocument doc = Util.rowToDoc("testdb_", primaryKeys, rowMap, "localhost", null);
			for (String propName : doc.getPropertyNames()) {
				Property prop = doc.findProperty(propName);
				LOG.info(propName + ":    " + prop.nextValue().toString());
			}
			assertEquals("6fd5643953e6e60188c93b89c71bc1808eb7edc2", doc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString());
			assertEquals("eb476c046da8b3e83081e3195923aba1dd9c6045", doc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString());
		} catch (DBException e) {
			fail("Could not generate DB document from row.");
		} catch (RepositoryException e) {
			fail("Could not generate DB document from row.");
		}
	}

	public final void testGetCheckpointString() throws DBException {
		Map<String, Object> rowMap = TestUtils.getStandardDBRow();
		DBDocument doc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), rowMap, "localhost", null);
		try {
			String checkpointStr = Util.getCheckpointString(null, null);
			assertEquals("(NO_TIMESTAMP)NO_DOCID", checkpointStr);
			DateTime dt = new DateTime();
			checkpointStr = Util.getCheckpointString(dt, doc);
			assertTrue(checkpointStr.contains(dt.toString()));
			assertTrue(checkpointStr.contains("6fd5643953e6e60188c93b89c71bc1808eb7edc2"));
			LOG.info(checkpointStr);
		} catch (RepositoryException e) {
			fail("Unexpected exception" + e.toString());
		}
	}
}
