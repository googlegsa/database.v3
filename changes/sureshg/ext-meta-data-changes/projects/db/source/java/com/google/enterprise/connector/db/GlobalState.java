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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.joda.time.DateTime;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;

/**
 * This is the main class for maintaining the global state which can be
 * persisted to XML and loaded again from it. It dumps the current values of the
 * cursor in the DB to execute the next query from and the generated checksum
 * maps. It also maintains a doc queue which holds the docs to be sent to the CM
 * when it asks for them. The docQueue is not saved in the state xml. Instead
 * these docs are fetched again when the state is loaded. Two hash maps -
 * previous and current are maintained which map docId to the checksum of the
 * doc contents from the previous and current sweep of the DB respectively. When
 * a query is executed, a doc gets added to the doc queue only if the doc has
 * changed or has been added since the previous sweep. Docs which are not found
 * in the latest sweep are marked for deletion.
 */
public class GlobalState {
	private static final Logger LOG = Logger.getLogger(GlobalState.class.getName());
	private static final String CONNECTOR_NAME = "DBConnector";
	private static final String STATEFILE_SUFFIX = "_state.xml";
	private static final String DB_CURSOR_XML = "dbCursor";
	private static final String PREVIOUS_CHECKSUM_MAP_XML = "previousChecksumMap";
	private static final String CURRENT_CHECKSUM_MAP_XML = "currentChecksumMap";
	private static final String CHECKSUM_MAP_ENTRY_XML = "checksumMapEntry";
	private static final String DOCID_XML = "docid";
	private static final String ROW_CHECKSUM_XML = "rowChecksum";
	private static final String STATE_XML = "state";
	private String workDir = null;
	private int cursorDB = 0;
	private HashMap<String, String> previousChecksumMap = new HashMap<String, String>();
	private HashMap<String, String> currentChecksumMap = new HashMap<String, String>();
	private DBDocumentList docQueue;
	private LinkedList<DBDocument> docsInFlight = new LinkedList<DBDocument>();
	private DateTime queryExecutionTime = null;
	private DateTime queryTimeForInFlightDocs = null;
	private static boolean isMetadataURLFeed = false;

	public GlobalState(String workDir) {
		this.workDir = workDir;
		docQueue = new DBDocumentList(this);
	}

	public DateTime getQueryTimeForInFlightDocs() {
		return queryTimeForInFlightDocs;
	}

	public void setQueryTimeForInFlightDocs(DateTime queryTimeForInFlightDocs) {
		this.queryTimeForInFlightDocs = queryTimeForInFlightDocs;
	}

	public DateTime getQueryExecutionTime() {
		return queryExecutionTime;
	}

	public void setQueryExecutionTime(DateTime dt) {
		this.queryExecutionTime = dt;
	}

	LinkedList<DBDocument> getDocsInFlight() {
		return docsInFlight;
	}

	public int getCursorDB() {
		return cursorDB;
	}

	public void setCursorDB(int cursorDB) {
		this.cursorDB = cursorDB;
	}

	public Map<String, String> getPreviousChecksumMap() {
		return Collections.unmodifiableMap(previousChecksumMap);
	}

	// This is package private so that unittests can access it.
	void setPreviousChecksumMap(Map<String, String> checksumMap) {
		previousChecksumMap.clear();
		previousChecksumMap.putAll(checksumMap);
	}

	public Map<String, String> getCurrentChecksumMap() {
		return Collections.unmodifiableMap(currentChecksumMap);
	}

	public DBDocumentList getDocQueue() {
		return docQueue;
	}

	/**
	 * It marks the starts of a new DB Traversal which means the traversal has
	 * to start from the first row. Documents in the previous checksum map are
	 * added to the doc queue for the deletion.
	 */
	public void markNewDBTraversal() {

		// mark documents for DELETE only for Content feed. Otherwise just
		// clear the entries from "previousChecksumMap".
		if (!isMetadataURLFeed) {
			addDocumentsToDelete();
		} else {
			previousChecksumMap.clear();
		}
		previousChecksumMap.putAll(currentChecksumMap);
		currentChecksumMap.clear();
		setCursorDB(0);
	}

	public static boolean isMetadataURLFeed() {
		return isMetadataURLFeed;
	}

	public static void setMetadataURLFeed(boolean isMetadataURLFeed) {
		GlobalState.isMetadataURLFeed = isMetadataURLFeed;
	}

	/**
	 * Gets the next document from the queue.
	 * 
	 * @return next document from the docQueue.
	 */
	public Document nextDocument() {
		return docQueue.nextDocument();
	}

	/**
	 * Adds all the docs in the previous checksum Map for deletion to the doc
	 * queue.
	 */
	private void addDocumentsToDelete() {
		LOG.info(previousChecksumMap.size()
				+ " document(s) are marked for delete feed");
		for (String key : previousChecksumMap.keySet()) {
			DBDocument dbDoc = new DBDocument();
			dbDoc.setProperty(SpiConstants.PROPNAME_DOCID, key);
			dbDoc.setProperty(DBDocument.ROW_CHECKSUM, previousChecksumMap.get(key));
			dbDoc.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.DELETE.toString());
			docQueue.addDocument(dbDoc);
		}
		previousChecksumMap.clear();
	}

	/**
	 * Adds a document only if it has changed or is new.
	 * 
	 * @param dbDoc document to add.
	 * @throws DBException
	 */
	public void addDocument(DBDocument dbDoc) throws DBException {
		try {
			String docId = dbDoc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString();
			String newRowChecksum = dbDoc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString();
			String oldRowChecksum = previousChecksumMap.get(docId);
			if (oldRowChecksum == null) {
				docQueue.addDocument(dbDoc);
			} else if (newRowChecksum.equals(oldRowChecksum)) {
				previousChecksumMap.remove(docId);
			} else {
				docQueue.addDocument(dbDoc);
				previousChecksumMap.remove(docId);
			}
			currentChecksumMap.put(docId, newRowChecksum);
		} catch (RepositoryException e) {
			throw new DBException("Could not add document to the checksum Map");
		}
	}

	/**
	 * Gets the location of the state file.
	 * 
	 * @return state file location.
	 */
	public File getStateFileLocation() {
		File f;
		if (workDir == null) {
			LOG.info("workDir is null; using cwd");
			f = new File(CONNECTOR_NAME + STATEFILE_SUFFIX);
		} else {
			f = new File(workDir, CONNECTOR_NAME + STATEFILE_SUFFIX);
		}
		return f;
	}

	/**
	 * Saves the state into an xml file. The state file looks something like
	 * this:
	 * 
	 * <pre>
	 * &lt;?xml version=&quot;1.0&quot; encoding=&quot;UTF-8&quot;standalone=&quot;no&quot;?&gt;
	 *   &lt;state&gt;
	 *   &lt;dbCursor&gt;0&lt;/dbCursor&gt;
	 *   &lt;currentChecksumMap&gt;
	 *     &lt;checksumMapEntry docid=&quot;docId3&quot; rowChecksum=&quot;rowChecksum3&quot;/&gt;
	 *     &lt;checksumMapEntry docid=&quot;docId4&quot; rowChecksum=&quot;rowChecksum4&quot;/&gt;
	 *   &lt;/currentChecksumMap&gt;
	 *   &lt;previousChecksumMap&gt;
	 *     &lt;checksumMapEntry docid=&quot;docId1&quot; rowChecksum=&quot;rowChecksum1&quot;/&gt;
	 *     &lt;checksumMapEntry docid=&quot;docId2&quot; rowChecksum=&quot;rowChecksum2&quot;/&gt;
	 *   &lt;/previousChecksumMap&gt;
	 * &lt;/state&gt;
	 * </pre>
	 * 
	 * @throws DBException
	 */
	public void saveState() throws DBException {
		try {
			String xml = dumpToStateXML();
			File f = getStateFileLocation();

			FileOutputStream out = new FileOutputStream(f);
			out.write(xml.getBytes("UTF-8"));
			out.close();
			LOG.info("Saving state to " + f.getCanonicalPath());
		} catch (IOException e) {
			throw new DBException("Could not save state.");
		}
	}

	/**
	 * Creates the xml string corresponding to the current state.
	 * 
	 * @return xml string.
	 * @throws DBException
	 */
	private String dumpToStateXML() throws DBException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		org.w3c.dom.Document doc;
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			doc = builder.newDocument();
		} catch (ParserConfigurationException e) {
			throw new DBException("Unable to get XML for state.");
		}
		Element top = doc.createElement(STATE_XML);
		doc.appendChild(top);

		int numDocs = 0;
		int numDocsToDelete = 0;
		HashMap<String, String> tempPreviousChecksumMap = new HashMap<String, String>();

		// Iterate through docQueue and docsInFlight to find out num of
		// documents
		// whose action is set to DELETE and create temporary list of these
		// docs.
		// This list will be added to previousChecksumMap when persisting to the
		// disk.
		for (DBDocument dbDoc : docQueue.getDocList()) {
			numDocs++;
			Property prop = dbDoc.findProperty(SpiConstants.PROPNAME_ACTION);
			if (prop != null) {
				try {
					if (dbDoc.findProperty(SpiConstants.PROPNAME_ACTION).nextValue().toString().equals(SpiConstants.ActionType.DELETE.toString())) {
						numDocsToDelete++;
						tempPreviousChecksumMap.put(dbDoc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString(), dbDoc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString());
					}
				} catch (RepositoryException e) {
					throw new DBException(
							"Could not get ActionType for document", e);
				}
			}
		}
		for (DBDocument dbDoc : docsInFlight) {
			numDocs++;
			Property prop = dbDoc.findProperty(SpiConstants.PROPNAME_ACTION);
			if (prop != null) {
				try {
					if (prop.nextValue().toString().equals(SpiConstants.ActionType.DELETE.toString())) {
						numDocsToDelete++;
						tempPreviousChecksumMap.put(dbDoc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString(), dbDoc.findProperty(DBDocument.ROW_CHECKSUM).nextValue().toString());
					}
				} catch (RepositoryException e) {
					throw new DBException(
							"Could not get ActionType for document", e);
				}
			}
		}

		// Add cursor in the DB till which the rows have been fetched. Since the
		// doc queue is not saved, so cursor needs to retract a bit.
		Element dbCursorElement = doc.createElement(DB_CURSOR_XML);
		int savedCursorDB = 0;
		if ((cursorDB - numDocs + numDocsToDelete) > 0) {
			// docQueue can have documents to delete also.
			savedCursorDB = cursorDB - numDocs + numDocsToDelete;
		}
		dbCursorElement.appendChild(doc.createTextNode(Integer.toString(savedCursorDB)));
		top.appendChild(dbCursorElement);

		// Dump currentChecksumMap.
		Element currentChecksumMapElement = doc.createElement(CURRENT_CHECKSUM_MAP_XML);
		appendChecksumMapXmlElement(currentChecksumMapElement, doc, currentChecksumMap);
		top.appendChild(currentChecksumMapElement);

		// Dump previousChecksumMap.
		Element previousChecksumMapElement = doc.createElement(PREVIOUS_CHECKSUM_MAP_XML);
		appendChecksumMapXmlElement(previousChecksumMapElement, doc, previousChecksumMap);
		appendChecksumMapXmlElement(previousChecksumMapElement, doc, tempPreviousChecksumMap);
		top.appendChild(previousChecksumMapElement);

		String stateXml = null;
		try {
			stateXml = XmlUtils.getStringFromDomDocument(doc, null);
		} catch (TransformerException e) {
			throw new DBException(" Unable to get XML for state.");
		}
		return stateXml;
	}

	private void appendChecksumMapXmlElement(Element checksumMapElement,
			org.w3c.dom.Document doc, Map<String, String> checksumMap) {
		for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
			Element checksumMapEntryElement = doc.createElement(CHECKSUM_MAP_ENTRY_XML);
			checksumMapEntryElement.setAttribute(DOCID_XML, entry.getKey());
			checksumMapEntryElement.setAttribute(ROW_CHECKSUM_XML, entry.getValue());
			checksumMapElement.appendChild(checksumMapEntryElement);
		}
	}

	/**
	 * Loads state into the gloablState object from the state file.
	 */
	public void loadState() {
		File f = getStateFileLocation();
		try {
			if (!f.exists() || f == null) {
				LOG.warning("Could not find state file. Starting all over again.");
			}
			LOG.info("Loading state from " + f.getCanonicalPath());
			loadFromStateXML(f);
		} catch (IOException e) {
			LOG.warning("Could not find state file. Starting all over again.");
		} catch (DBException e) {
			LOG.warning("Could not load state. Starting all over again");
		}
	}

	private void loadFromStateXML(File f) throws DBException {
		docQueue.clear();
		docsInFlight.clear();
		previousChecksumMap.clear();
		currentChecksumMap.clear();
		cursorDB = 0;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document doc = builder.parse(f);
			NodeList stateNodeList = doc.getElementsByTagName("state");
			if ((stateNodeList == null) || (stateNodeList.getLength() == 0)) {
				throw new DBException("Invalid XML: no <state> element");
			}
			if (stateNodeList.item(0) == null) {
				throw new DBException(
						"Unable to get the item of the nodelist for "
								+ "tag '<state>'");
			}

			NodeList cursorDBNodeList = ((Element) stateNodeList.item(0)).getElementsByTagName(DB_CURSOR_XML);
			if ((cursorDBNodeList == null)
					|| (cursorDBNodeList.getLength() == 0)) {
				throw new DBException("Unable to get cursorDB");
			}
			if (cursorDBNodeList.item(0) == null) {
				throw new DBException("Unable to get value of cursorDB");
			}
			cursorDB = Integer.parseInt(cursorDBNodeList.item(0).getTextContent());

			currentChecksumMap.clear();
			NodeList currentChecksumMapNodeList = ((Element) stateNodeList.item(0)).getElementsByTagName(CURRENT_CHECKSUM_MAP_XML);
			if ((currentChecksumMapNodeList == null)
					|| (currentChecksumMapNodeList.getLength() == 0)) {
				throw new DBException("Unable to get currentChecksumMap");
			}
			if (currentChecksumMapNodeList.item(0) == null) {
				throw new DBException(
						"Unable to get value of currentChecksumMap");
			}
			loadChecksumMaps(currentChecksumMapNodeList, true);

			previousChecksumMap.clear();
			NodeList previousChecksumMapNodeList = ((Element) stateNodeList.item(0)).getElementsByTagName(PREVIOUS_CHECKSUM_MAP_XML);
			if ((previousChecksumMapNodeList == null)
					|| (previousChecksumMapNodeList.getLength() == 0)) {
				throw new DBException("Unable to get previousChecksumMap");
			}
			if (previousChecksumMapNodeList.item(0) == null) {
				throw new DBException(
						"Unable to get value of previousChecksumMap");
			}
			loadChecksumMaps(previousChecksumMapNodeList, false);
		} catch (ParserConfigurationException e) {
			throw new DBException("Could not load state from state file", e);
		} catch (SAXException e1) {
			throw new DBException("Could not parse state xml.", e1);
		} catch (IOException e2) {
			throw new DBException("Could not read state file", e2);
		}
	}

	private void loadChecksumMaps(NodeList checksumMapNodeList, boolean current) {
		NodeList checksumMapEntryNodeList = ((Element) checksumMapNodeList.item(0)).getElementsByTagName(CHECKSUM_MAP_ENTRY_XML);
		if (checksumMapEntryNodeList != null) {
			for (int i = 0; i < checksumMapEntryNodeList.getLength(); i++) {
				Element checksumMapEntryElement = (Element) checksumMapEntryNodeList.item(i);
				String docId = checksumMapEntryElement.getAttribute(DOCID_XML);
				String rowChecksum = checksumMapEntryElement.getAttribute(ROW_CHECKSUM_XML);
				if (docId != null && rowChecksum != null) {
					if (current) {
						currentChecksumMap.put(docId, rowChecksum);
					} else {
						previousChecksumMap.put(docId, rowChecksum);
					}
				}
			}
		}
	}

	/**
	 * Clears the state.
	 * 
	 * @throws DBException
	 */
	public void clearState() throws DBException {
		File f = getStateFileLocation();
		LOG.info("deleting state file " + f.getAbsolutePath());
		if (f.exists()) {
			if (f.delete()) {
				LOG.info("successfully deleted");
			} else {
				throw new DBException("Could not clear state.");
			}
		} else {
			LOG.info("state file does not exist.");
		}
		docQueue.clear();
		previousChecksumMap.clear();
		currentChecksumMap.clear();
		cursorDB = 0;
	}
}
