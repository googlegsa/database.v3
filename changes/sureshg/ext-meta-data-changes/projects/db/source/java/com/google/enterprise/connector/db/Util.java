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
import com.google.enterprise.connector.spi.TraversalContext;

import org.joda.time.DateTime;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Utility class for database connector.
 */
public class Util {
	public static final String NO_TIMESTAMP = "NO_TIMESTAMP";
	public static final String NO_DOCID = "NO_DOCID";
	private static final Logger LOG = Logger.getLogger(Util.class.getName());
	public static final String PRIMARY_KEYS_SEPARATOR = ",";
	private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
	private static final String CHECKSUM_ALGO = "SHA1";
	private static final String MIMETYPE = "text/html";
	private static final String DBCONNECTOR_PROTOCOL = "dbconnector://";
	private static final String DATABASE_TITLE_PREFIX = "Database Connector Result";

	private static TraversalContext context = null;
	public static String WITH_BASE_URL = "withBaseURL";

	// This class should not be initialized.
	private Util() {
	}

	/**
	 * Converts a row to document. Docid is the checksum of primary keys values,
	 * concatenated with a comma. Content is the xml representation of a row. It
	 * also adds the checksum of the contents.
	 * 
	 * @param row row of a table.
	 * @return doc
	 * @throws DBException
	 */
	public static DBDocument rowToDoc(String dbName, String[] primaryKeys,
			Map<String, Object> row, String hostname, String xslt,
			DBContext dbContext) throws DBException {

		// TODO(meghna): Look into what other document properties can be added.
		DBDocument doc = new DBDocument();

		String contentXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
		doc.setProperty(SpiConstants.PROPNAME_CONTENT, contentXMLRow);
		String docId = generateDocId(primaryKeys, row);
		doc.setProperty(SpiConstants.PROPNAME_DOCID, docId);
		doc.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());
		// TODO(meghna): Look into which encoding/charset to use for getBytes()
		String completeXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, true);
		doc.setProperty(DBDocument.ROW_CHECKSUM, getChecksum(completeXMLRow.getBytes()));
		doc.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);
		doc.setProperty(SpiConstants.PROPNAME_DISPLAYURL, getDisplayUrl(hostname, dbName, docId));

		// set feed type as content feed
		doc.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.CONTENT.toString());

		/*
		 * Set other doc properties
		 */
		setOptionalProperties(row, doc, dbContext);

		return doc;
	}

	private static String getDisplayUrl(String hostname, String dbName,
			String docId) {
		// displayurl is of the form -
		// dbconnector://meghna-linux.corp.google.com/mysql/2a61639c96ed45ec8f6e3d4e1ab79944cd1d1923
		String displayUrl = String.format("%s%s/%s/%s", DBCONNECTOR_PROTOCOL, hostname, dbName, docId);
		return displayUrl;
	}

	/**
	 * Generates the title of the DB document.
	 * 
	 * @param primaryKeys primary keys of the database.
	 * @param row row corresponding to the document.
	 * @return title String.
	 */
	public static String getTitle(String[] primaryKeys, Map<String, Object> row)
			throws DBException {
		StringBuilder title = new StringBuilder();
		title.append(DATABASE_TITLE_PREFIX).append(" ");

		if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
			Set<String> keySet = row.keySet();
			for (String primaryKey : primaryKeys) {
				/*
				 * Primary key value is mapped to the value of key of map row
				 * before getting record. We need to do this because GSA admin
				 * may entered primary key value which differed in case from
				 * column name.
				 */
				for (String key : keySet) {
					if (primaryKey.equalsIgnoreCase(key)) {
						primaryKey = key;
						break;
					}
				}
				if (!keySet.contains(primaryKey)) {
					String msg = "Primary Key does not match with any of the coulmn names";
					LOG.info(msg);
					throw new DBException(msg);
				}
				Object keyValue = row.get(primaryKey);
				String strKeyValue;
				if (keyValue == null
						|| keyValue.toString().trim().length() == 0) {
					strKeyValue = "";
				} else {
					strKeyValue = keyValue.toString();
				}
				title.append(primaryKey).append("=");
				title.append(strKeyValue).append(" ");
			}
		} else {
			String msg = "";
			if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
				msg = "row is null and primary key array is empty";
			} else if (row != null) {
				msg = "hash map row is null";
			} else {
				msg = "primary key array is empty or null";
			}
			LOG.info(msg);
			throw new DBException(msg);
		}
		return title.toString();
	}

	/**
	 * Generates the docId for a DB row. If the primary keys are id and lastName
	 * and their corresponding values are 1 and last_01, then the docId would be
	 * the SHA1 checksum of (1,7)1last_01. The key values are concatenated and
	 * is prepended with their lengths in parentheses.
	 * 
	 * @return docId checksum generated using the primary key values.
	 */
	public static String generateDocId(String[] primaryKeys,
			Map<String, Object> row) throws DBException {
		StringBuilder length = new StringBuilder();
		StringBuilder primaryKeyValues = new StringBuilder();
		length.append("(");
		if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
			Set<String> keySet = row.keySet();

			for (String primaryKey : primaryKeys) {
				/*
				 * Primary key value is mapped to the value of key of map row
				 * before getting record. We need to do this because GSA admin
				 * may entered primary key value which differed in case from
				 * column name.
				 */

				for (String key : keySet) {
					if (primaryKey.equalsIgnoreCase(key)) {
						primaryKey = key;
						break;
					}
				}
				if (!keySet.contains(primaryKey)) {
					String msg = "Primary Key does not match with any of the coulmn names";
					LOG.info(msg);
					throw new DBException(msg);
				}
				Object keyValue = row.get(primaryKey);
				if (null == keyValue) {
					length.append("-1" + PRIMARY_KEYS_SEPARATOR);
				} else {
					String keyValueStr = keyValue.toString();
					length.append(keyValueStr.length() + PRIMARY_KEYS_SEPARATOR);
					primaryKeyValues.append(keyValueStr);
				}
			}
		} else {
			String msg = "";
			if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
				msg = "row is null and primary key array is empty";
			} else if (row != null) {
				msg = "hash map row is null";
			} else {
				msg = "primary key array is empty or null";
			}
			LOG.info(msg);
			throw new DBException(msg);
		}
		length.deleteCharAt(length.length() - 1);
		length.append(")");
		length.append(primaryKeyValues.toString());
		LOG.info("Primary key values concatenated string : "
				+ length.toString());
		String docId = getChecksum(length.toString().getBytes());
		LOG.info("DocId : " + docId);
		return docId;
	}

	/**
	 * Generates the SHA1 checksum.
	 * 
	 * @param buf
	 * @return checksum string.
	 */
	private static String getChecksum(byte[] buf) {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance(CHECKSUM_ALGO);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Could not get a message digest for "
					+ CHECKSUM_ALGO);
		}
		digest.update(buf);
		return asHex(digest.digest());
	}

	/**
	 * Utility method to convert a byte[] to hex string.
	 * 
	 * @param buf
	 * @return hex string.
	 */
	private static String asHex(byte[] buf) {
		char[] chars = new char[2 * buf.length];
		for (int i = 0; i < buf.length; ++i) {
			chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
			chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
		}
		return new String(chars);
	}

	/**
	 * Creates a checkpoint string of the form (date_time_string)docId.
	 * 
	 * @param dt
	 * @param doc
	 * @return checkpoint string.
	 * @throws RepositoryException
	 */
	public static String getCheckpointString(DateTime dt, DBDocument doc)
			throws RepositoryException {
		StringBuilder str = new StringBuilder();
		str.append("(");
		if (null == dt) {
			str.append(NO_TIMESTAMP);
		} else {
			str.append(dt.toString());
		}
		str.append(")");
		if (null == doc) {
			str.append(NO_DOCID);
		} else {
			str.append(doc.findProperty(SpiConstants.PROPNAME_DOCID).nextValue().toString());
		}
		return str.toString();
	}

	/**
	 * This method set the values for predefined Document properties in
	 * DBDocument. For example PROPNAME_DISPLAYURL , PROPNAME_TITLE ,
	 * PROPNAME_LASTMODIFIED.
	 * 
	 * @param row Map representing database row
	 * @param doc DB Document
	 * @param hostname connector host name
	 * @param dbName database name
	 * @param docId document id of DB doc
	 * @param isContentFeed true if Feed type is content feed
	 */
	private static void setOptionalProperties(Map<String, Object> row,
			DBDocument doc, DBContext dbContext) {
		if (dbContext == null) {
			return;
		}
		// set Document Title
		Object docTitle = row.get(dbContext.getDocumentTitle());
		if (docTitle != null) {
			doc.setProperty(SpiConstants.PROPNAME_TITLE, docTitle.toString());
		}
		// set last modified date
		Object lastModified = row.get(dbContext.getLastModifiedDate());
		if (lastModified != null && (lastModified instanceof Timestamp)) {
			doc.setLastModifiedDate(SpiConstants.PROPNAME_LASTMODIFIED, (Timestamp) lastModified);
		}
	}

	/**
	 * This method convert given row into equivalent Metadata-URL feed. There
	 * could be two scenarios depending upon how we get the URL of document. In
	 * first scenario one of the column hold the complete URL of the document
	 * and other columns holds the metadata of primary document. The name of URL
	 * column is provided by user in configuration form. In second scenario the
	 * URL of primary document is build by concatenating the base url and
	 * document ID. COnnector admin provides the Base URL and document ID column
	 * in DB connector configuration form.
	 * 
	 * @param dbName Name of database
	 * @param primaryKeys array of primary key columns
	 * @param row map representing database row.
	 * @param hostname fully qualified connector hostname.
	 * @param dbContext instance of DBContext.
	 * @param type represent how to get URL of the document. If value is
	 *            "withBaseURL" it means we have to build document URL using
	 *            base URL and document ID.
	 * @return DBDocument
	 * @throws DBException
	 */
	public static DBDocument generateMetadataURLFeed(String dbName,
			String[] primaryKeys, Map<String, Object> row, String hostname,
			DBContext dbContext, String type) throws DBException {

		boolean isWithBaseURL = type.equalsIgnoreCase(Util.WITH_BASE_URL);

		/*
		 * skipColumns maintain the list of column which needs to skip while
		 * indexing as they are not part of metadata or they already considered
		 * for indexing. For example document_id column, MIME type column, URL
		 * columns.
		 */
		List<String> skipColumns = new ArrayList<String>();

		String baseURL = null;
		String docIdColumn = null;
		String finalURL = null;
		if (isWithBaseURL) {
			baseURL = (String) row.get(dbContext.getBaseURL());
			docIdColumn = dbContext.getDocumentIdField();
			finalURL = baseURL.trim() + row.get(docIdColumn);
			skipColumns.add(dbContext.getDocumentIdField());
		} else {
			skipColumns.add(dbContext.getDocumentURLField());
			finalURL = (String) row.get(dbContext.getDocumentURLField());
		}

		DBDocument doc = new DBDocument();
		// get doc id from primary key values
		String docId = generateDocId(primaryKeys, row);

		String xmlRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, null, dbContext, true);

		/*
		 * This method add addition database columns(last modified and doc
		 * title) which needs to skip while sending as metadata as they are
		 * already consider as metadata.
		 */
		skipOtherProperties(skipColumns, dbContext);

		doc.setProperty(SpiConstants.PROPNAME_SEARCHURL, finalURL);
		doc.setProperty(SpiConstants.PROPNAME_DISPLAYURL, finalURL);

		// Set feed type as metadata_url
		doc.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.WEB.toString());
		// set doc id
		doc.setProperty(SpiConstants.PROPNAME_DOCID, docId);

		doc.setProperty(DBDocument.ROW_CHECKSUM, getChecksum(xmlRow.getBytes()));
		/*
		 * set action as add. Even when contents are updated the we still we set
		 * action as add and GSA overrides the old copy with new updated one.
		 * Hence ADD action is applicable to both add and update
		 */
		doc.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());

		/*
		 * Set other doc properties like Last Modified date and document title.
		 */
		setOptionalProperties(row, doc, dbContext);
		skipColumns.addAll(Arrays.asList(primaryKeys));
		setMetaInfo(doc, row, skipColumns);

		return doc;
	}

	/**
	 * This method will add value of each column as metadata to Database
	 * document except the values of columns in skipColumns list.
	 * 
	 * @param doc
	 * @param row
	 * @param skipColumns list of columns needs to ignore while indexing
	 */
	private static void setMetaInfo(DBDocument doc, Map<String, Object> row,
			List<String> skipColumns) {
		// get all column names as key set
		Set<String> keySet = row.keySet();
		for (String key : keySet) {
			// set column value as metadata and column name as meta-name.
			if (!skipColumns.contains(key)) {
				Object value = row.get(key);
				if (value != null)
					doc.setProperty(key, value.toString());
			} else {
				LOG.info("skipping metadata indexing of column " + key);
			}
		}
	}

	/**
	 * This method converts Large Object(BLOB or CLOB) into equivalent
	 * DBDocument.
	 * 
	 * @param dbName
	 * @param primaryKeys
	 * @param row
	 * @param hostname
	 * @param largeObjectField
	 * @return DBDocument for BLOB/CLOB data
	 * @throws DBException
	 */
	public static DBDocument largeObjectToDoc(String dbName,
			String[] primaryKeys, Map<String, Object> row, String hostname,
			DBContext dbContext) throws DBException {

		// get doc id from primary key values
		String docId = generateDocId(primaryKeys, row);

		String clobValue = null;
		DBDocument doc = new DBDocument();
		String mimeType = "";

		/*
		 * skipColumns maintain the list of column which needs to skip while
		 * indexing as they are not part of metadata or they already considered
		 * for indexing. For example document_id column, MIME type column, URL
		 * columns.
		 */
		List<String> skipColumns = new ArrayList<String>();

		/*
		 * get the value of large object from map representing a row
		 */
		Object largeObject = row.get(dbContext.getLobField());
		skipColumns.add(dbContext.getLobField());

		/*
		 * check if large object data value for null.
		 */
		if (largeObject != null) {
			/*
			 * check if large object is of type BLOB from the the column names.
			 * If column name is "dbconn_blob" it means large object is of type
			 * BLOB else it is CLOB.
			 */
			if (largeObject instanceof byte[]) {

				doc.setBinaryContent(SpiConstants.PROPNAME_CONTENT, (byte[]) largeObject);
				/*
				 * First try to get the MIME type of this file from the result
				 * set. If it does not maintain a column for MIME type try to
				 * get the MIME type of this document(file stored as BLOB in
				 * database) using MIME type finder utility.
				 */

				mimeType = new MimeTypeFinder().find((byte[]) largeObject, context);

				// set mime type for this document
				doc.setProperty(SpiConstants.PROPNAME_MIMETYPE, mimeType);

				// get xml representation of document(exclude the BLOB column).
				Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
				String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);
				// get checksum of blob
				String blobCheckSum = Util.getChecksum((byte[]) largeObject);
				// get checksum of other column
				String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());
				// get checksum of blob object and other column
				String docCheckSum = Util.getChecksum((otherColumnCheckSum + blobCheckSum).getBytes());
				// set checksum of this document
				doc.setProperty(DBDocument.ROW_CHECKSUM, docCheckSum);
				LOG.info("BLOB Data found");

			} else {
				/*
				 * get the value of CLOB as StringBuilder. iBATIS returns char
				 * array or String for CLOB data depending upon Database.
				 */
				if (largeObject instanceof char[]) {
					clobValue = new String((char[]) largeObject);
				} else if (largeObject instanceof String) {
					clobValue = largeObject.toString();
				}
				if (clobValue != null) {
					doc.setProperty(SpiConstants.PROPNAME_CONTENT, clobValue.toString());
				} else {
					LOG.warning("Content of documnet " + docId + " is null");
				}
				doc.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);

				// get xml representation of document(exclude the CLOB column).
				Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
				String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);
				// get checksum of CLOB
				String clobCheckSum = Util.getChecksum(clobValue.toString().getBytes());
				// get checksum of other column
				String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());
				// get checksum of blob object and other column
				String docCheckSum = Util.getChecksum((otherColumnCheckSum + clobCheckSum).getBytes());
				// set checksum of this document
				doc.setProperty(DBDocument.ROW_CHECKSUM, docCheckSum);
				LOG.info("CLOB Data found");
			}
			/*
			 * If large object is null then send empty value.
			 */
		} else {
			Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
			String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);
			doc.setProperty(DBDocument.ROW_CHECKSUM, Util.getChecksum(xmlRow.getBytes()));
			LOG.warning("Conetent of Document " + docId + " has null value.");
		}

		// set doc id
		doc.setProperty(SpiConstants.PROPNAME_DOCID, docId);

		// set feed type as content feed
		doc.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.CONTENT.toString());

		// set action as add
		doc.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());

		/*
		 * if connector admin has has provided Fetch URL column the use the
		 * value of this column as a "Display URL". Else construct the display
		 * URL and use it.
		 */
		Object displayURL = row.get(dbContext.getFetchURLField());
		if (displayURL != null && displayURL.toString().trim().length() > 0) {
			doc.setProperty(SpiConstants.PROPNAME_DISPLAYURL, displayURL.toString().trim());
			skipColumns.add(displayURL.toString());
		} else {
			doc.setProperty(SpiConstants.PROPNAME_DISPLAYURL, getDisplayUrl(hostname, dbName, docId));
		}

		skipOtherProperties(skipColumns, dbContext);
		skipColumns.addAll(Arrays.asList(primaryKeys));

		setOptionalProperties(row, doc, dbContext);

		setMetaInfo(doc, row, skipColumns);

		/*
		 * Set other doc properties
		 */

		return doc;
	}

	/**
	 * this method copies all elements from map representing a row except BLOB
	 * column and return the resultant map.
	 * 
	 * @param row
	 * @return map representing a database table row.
	 */
	private static Map<String, Object> getRowForXmlDoc(Map<String, Object> row,
			DBContext dbContext) {
		Set<String> keySet = row.keySet();
		Map<String, Object> map = new HashMap<String, Object>();
		for (String key : keySet) {
			if (!dbContext.getLobField().equals(key)) {
				map.put(key, row.get(key));
			}
		}
		return map;
	}

	/**
	 * Set TraversalContext. TraversalContext is required for detecting
	 * appropriate MIME type of document
	 * 
	 * @param trContext
	 */
	public static void setTraversalContext(TraversalContext trContext) {
		context = trContext;
	}

	/**
	 * This method extract the columns for Last Modified date and Document Title
	 * and add in list of skip columns.
	 * 
	 * @param skipColumns list of columns to be skipped as metadata
	 * @param dbContext
	 */

	private static void skipOtherProperties(List<String> skipColumns,
			DBContext dbContext) {
		String lastModColumn = dbContext.getLastModifiedDate();
		String docTitle = dbContext.getDocumentTitle();
		if (lastModColumn != null && lastModColumn.trim().length() > 0) {
			skipColumns.add(lastModColumn);
		}
		if (docTitle != null && docTitle.trim().length() > 0) {
			skipColumns.add(docTitle);
		}
	}

}
