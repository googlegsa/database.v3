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

import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class GlobalStateTest extends TestCase {
	private static final Logger LOG = Logger.getLogger(GlobalStateTest.class.getName());
	private GlobalState globalState;
	private TestDirectoryManager testDirManager;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		testDirManager = new TestDirectoryManager(this);
		globalState = new GlobalState(testDirManager.getTmpDir());
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public final void testLoadState() throws Exception {
		GlobalState currentGlobalState = new GlobalState(
				testDirManager.getTestDataDir());
		currentGlobalState.setCursorDB(20);
		assertEquals(20, currentGlobalState.getCursorDB());
		currentGlobalState.loadState();
		assertEquals(0, currentGlobalState.getCursorDB());
		LOG.info("currentchecksumMap : ");
		for (String key : currentGlobalState.getCurrentChecksumMap().keySet()) {
			LOG.info("docId : " + key + "    " + "rowChecksum : "
					+ currentGlobalState.getCurrentChecksumMap().get(key));
		}
		assertTrue(currentGlobalState.getCurrentChecksumMap().containsKey("docId3"));
		LOG.info("previousChecksumMap : ");
		for (String key : currentGlobalState.getPreviousChecksumMap().keySet()) {
			LOG.info("docId : " + key + "    " + "rowChecksum : "
					+ currentGlobalState.getPreviousChecksumMap().get(key));
		}
		assertTrue(currentGlobalState.getPreviousChecksumMap().containsKey("docId1"));
		assertEquals(0, currentGlobalState.getDocQueue().size());
	}

	public final void testAddDocument() {
		try {
			// Add 4 documents.
			for (Map<String, Object> row : TestUtils.getDBRows()) {
				DBDocument dbDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), row, "localhost", null, null);
				globalState.addDocument(dbDoc);
			}

			globalState.setCursorDB(globalState.getDocQueue().size());
			assertEquals(4, globalState.getCurrentChecksumMap().size());
			assertEquals(0, globalState.getPreviousChecksumMap().size());
			assertEquals(4, globalState.getDocQueue().size());
			for (int i = 0; i < 4; i++) {
				// Consume all docs from the docQueue.
				globalState.getDocQueue().nextDocument();
			}
			globalState.markNewDBTraversal();
			assertEquals(0, globalState.getDocQueue().size());
			assertEquals(4, globalState.getPreviousChecksumMap().size());
			assertEquals(0, globalState.getCurrentChecksumMap().size());
			assertEquals(0, globalState.getCursorDB());
			assertEquals(4, globalState.getDocsInFlight().size());

			DBDocument dbDoc = TestUtils.createDBDoc(1, "first_01", "last_01", "01@google.com");

			// Add a previously added document. Should get removed from previous
			// map,
			// added in the current map but not added in the doc queue.
			globalState.addDocument(dbDoc);
			assertEquals(0, globalState.getDocQueue().size());
			assertEquals(3, globalState.getPreviousChecksumMap().size());
			assertEquals(1, globalState.getCurrentChecksumMap().size());

			LOG.info(globalState.getPreviousChecksumMap().keySet().toString());

			// Add an updated document. Should get removed from previous map,
			// added in the current map and to the doc queue.
			dbDoc = TestUtils.createDBDoc(2, "first_05", "last_02", "05@google.com");
			LOG.info(dbDoc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString());
			globalState.addDocument(dbDoc);
			assertEquals(1, globalState.getDocQueue().size());
			assertEquals(2, globalState.getPreviousChecksumMap().size());
			assertEquals(2, globalState.getCurrentChecksumMap().size());

			// Add a new document. Should get added in the current map and to
			// the doc queue.
			dbDoc = TestUtils.createDBDoc(5, "first_05", "last_05", "05@google.com");
			globalState.addDocument(dbDoc);
			assertEquals(2, globalState.getDocQueue().size());
			assertEquals(2, globalState.getPreviousChecksumMap().size());
			assertEquals(3, globalState.getCurrentChecksumMap().size());

			for (int i = 0; i < 2; i++) {
				// Consume all docs from the docQueue.
				globalState.getDocQueue().nextDocument();
			}
			assertEquals(0, globalState.getDocQueue().size());
			assertEquals(6, globalState.getDocsInFlight().size());

			globalState.markNewDBTraversal();
			// docs in the previous checksum map should get marked for deletion.
			assertEquals(2, globalState.getDocQueue().size());
			assertEquals(3, globalState.getPreviousChecksumMap().size());

			dbDoc = (DBDocument) globalState.getDocQueue().nextDocument();
			assertEquals(SpiConstants.ActionType.DELETE.toString(), dbDoc.findProperty(SpiConstants.PROPNAME_ACTION).nextValue().toString());
		} catch (DBException e) {
			fail("Caught exception");
		} catch (RepositoryException e1) {
			fail("Caught exception");
		}
	}

	public final void testSaveState() {
		String[] expectedPatterns = new String[] { "docid=\"MyxsYXN0XzAz.*\" ",
				"</currentChecksumMap><previousChecksumMap><checksumMapEntry ",
				"docid=\"NCxsYXN0XzA0.*\"", "<checksumMapEntry" };
		String expectedCursorXml = "<dbCursor>0</dbCursor>";
		String[] expectedDocToDeleteXml = new String[] {
				"docid=\"MSxsYXN0XzAx.*\"", "rowChecksum=.*" };

		try {
			for (Map<String, Object> row : TestUtils.getDBRows()) {
				DBDocument dbDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), row, "localhost", null, null);
				globalState.addDocument(dbDoc);
			}
			globalState.setCursorDB(4);
			globalState.setPreviousChecksumMap(globalState.getCurrentChecksumMap());
			// Consume 1 doc. It should be in the docsInFlight.
			globalState.getDocQueue().nextDocument();

			// Set the last document in the docQueue to delete.
			globalState.saveState();

			String actualXml = readFile(globalState.getStateFileLocation());
			LOG.info(actualXml);
			assertCheckPatterns(actualXml, expectedPatterns);
			// assertTrue(actualXml.toString().contains(expectedXmlSnippet));
			assertTrue(actualXml.toString().contains(expectedCursorXml));

			DBDocument dbDoc = TestUtils.createDBDoc(5, "first_05", "last_05", "05@google.com");
			Map<String, String> tempMap = new HashMap<String, String>();
			try {
				tempMap.put(dbDoc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString(), dbDoc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString());

				globalState.setPreviousChecksumMap(tempMap);
			} catch (RepositoryException e) {
				fail("Caught exception");
			}
			assertEquals(1, globalState.getPreviousChecksumMap().size());
			globalState.markNewDBTraversal();
			assertEquals(4, globalState.getPreviousChecksumMap().size());
			globalState.saveState();
			actualXml = readFile(globalState.getStateFileLocation());
			LOG.info(actualXml);
			assertCheckPatterns(actualXml, expectedDocToDeleteXml);
		} catch (DBException e) {
			fail("Caught exception");
		}
	}

	private String readFile(File f) {
		StringBuilder str = new StringBuilder();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			char[] buf = new char[1024];
			int numRead = 0;
			while ((numRead = reader.read(buf)) != -1) {
				String readData = String.valueOf(buf, 0, numRead);
				str.append(readData);
			}
			reader.close();
		} catch (IOException e) {
			fail("Could not read from the state file");
		}
		return str.toString();
	}

	private void setUpInitialState() {
		try {
			for (Map<String, Object> row : TestUtils.getDBRows()) {
				DBDocument dbDoc = Util.rowToDoc("testdb_", TestUtils.getStandardPrimaryKeys(), row, "localhost", null, null);
				globalState.addDocument(dbDoc);
			}
		} catch (DBException e) {
			fail("Caught exception");
		}
	}

	public final void testSavedCursorDB() {
		setUpInitialState();
		globalState.setCursorDB(globalState.getDocQueue().size());
		assertEquals(4, globalState.getCursorDB());

		// Consume 2 documents.
		for (int i = 0; i < 2; i++) {
			globalState.getDocQueue().nextDocument();
		}
		try {
			globalState.saveState();
			globalState.loadState();
			// 2 in docQueue and 2 in inFlightDocs.
			assertEquals(0, globalState.getCursorDB());

			setUpInitialState();
			globalState.setCursorDB(4);
			// Consume 2 documents.
			for (int i = 0; i < 2; i++) {
				globalState.getDocQueue().nextDocument();
			}
			DBDocument dbDoc = globalState.getDocsInFlight().poll();
			dbDoc.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.DELETE.toString());
			globalState.getDocsInFlight().add(dbDoc);
			globalState.saveState();
			globalState.loadState();
			// 2 in docQueue and 2 in inFlightDocs with 1 doc to delete.
			assertEquals(1, globalState.getCursorDB());

		} catch (DBException e) {
			fail("Could not save state");
		}
	}

	/**
	 * Method search for pattern in document string. It gives an assertion error
	 * when accepted pattern does not found in document string.
	 * 
	 * @param docStringUnderTest String that represent actual document.
	 * @param expectedPatterns array of patterns that document String should
	 *            have
	 */
	private void assertCheckPatterns(final String docStringUnderTest,
			final String[] expectedPatterns) {

		Pattern pattern = null;
		Matcher match = null;

		for (String strPattern : expectedPatterns) {
			LOG.info("Checking for pattern  :   " + strPattern + "  ...");
			pattern = Pattern.compile(strPattern);
			match = pattern.matcher(docStringUnderTest);
			assertTrue(match.find());
		}
	}
}
