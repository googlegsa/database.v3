//Copyright 2011 Google Inc.
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
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

public class DBDocumentTest extends TestCase {

	public void testFindProperty() {
		Map<String, Object> rowMap = TestUtils.getStandardDBRow();
		String[] primaryKeys = TestUtils.getStandardPrimaryKeys();
		try {
			ProductionTraversalContext context = new ProductionTraversalContext();
			DBDocument doc = Util.rowToDoc("testdb_", primaryKeys, rowMap, "localhost", null, null, context);
			assertEquals("MSxsYXN0XzAx", doc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString());
			assertEquals("7ffd1d7efaf0d1ee260c646d827020651519e7b0", doc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString());
		} catch (DBException e) {
			fail("Could not generate DB document from row.");
		} catch (RepositoryException e) {
			fail("Could not generate DB document from row.");
		}
	}

	public void testGetPropertyNames() {
		Map<String, Object> rowMap = TestUtils.getStandardDBRow();
		String[] primaryKeys = TestUtils.getStandardPrimaryKeys();
		ProductionTraversalContext context = new ProductionTraversalContext();
		try {
			DBDocument doc = Util.rowToDoc("testdb_", primaryKeys, rowMap, "localhost", null, null, context);
			Set<String> propNames = doc.getPropertyNames();
			assertTrue(propNames.contains("dbconnector:checksum"));
		} catch (DBException e) {
			fail("Could not generate DB document from row.");
		}
	}

	public void testSetProperty() {
		ProductionTraversalContext context = new ProductionTraversalContext();
		DBDocument doc = new DBDocument(context);

		doc.setProperty(SpiConstants.PROPNAME_DOCID, "MSxsYXN0XzAx");
		try {
			assertEquals("MSxsYXN0XzAx", doc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString());
		} catch (SkippedDocumentException e) {
			fail("Skipped Document in Find property");
		} catch (RepositoryException e) {
			fail("Repository Exception");
		}
	}

	public void testSetLastModifiedDate() {
		ProductionTraversalContext context = new ProductionTraversalContext();
		DBDocument doc = new DBDocument(context);

		doc.setLastModifiedDate(SpiConstants.PROPNAME_LASTMODIFIED, Timestamp.valueOf("2011-05-15 22:11:33"));
		try {
			assertEquals("2011-05-15T22:11:33.000+0530", doc.findProperty(SpiConstants.PROPNAME_LASTMODIFIED).nextValue().toString());
		} catch (SkippedDocumentException e) {
			fail("Skipped Document in Find property");
		} catch (RepositoryException e) {
			fail("Repository Exception");
		}
	}

}
