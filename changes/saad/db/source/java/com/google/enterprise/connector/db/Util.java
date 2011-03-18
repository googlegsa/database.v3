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


import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.JsonObjectUtil;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
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
    public static final String ROW_CHECKSUM = "dbconnector:checksum";

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
    public static JsonDocument rowToDoc(String dbName, String[] primaryKeys,
            Map<String, Object> row, String hostname, String xslt,
            DBContext dbContext, TraversalContext context) throws DBException {

        JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

        String contentXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT, contentXMLRow);
        String docId = DocIdUtil.generateDocId(primaryKeys, row);

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());

        // TODO: Look into which encoding/charset to use for getBytes()
        String completeXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, true);
        jsonObjectUtil.setProperty(ROW_CHECKSUM, getChecksum(completeXMLRow.getBytes()));

        // set "ispublic" false if authZ query is provided by the user.
        if (dbContext != null && !dbContext.isPublicFeed()) {
            jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");

        }

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, getDisplayUrl(hostname, dbName, docId));

        // set feed type as content feed
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.CONTENT.toString());

        /*
         * Set other doc properties
         */
        setOptionalProperties(row, jsonObjectUtil, dbContext);
        JsonDocument jsonDoc = new JsonDocument(jsonObjectUtil.getJsonObject());
        return jsonDoc;
    }

    private static String getDisplayUrl(String hostname, String dbName,
            String docId) {
        // displayurl is of the form -
        // dbconnector://example.com/mysql/2a61639c96ed45ec8f6e3d4e1ab79944cd1d1923
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
					+ CHECKSUM_ALGO + "\n" + e);
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
     * This method set the values for predefined Document properties For example
     * PROPNAME_DISPLAYURL , PROPNAME_TITLE , PROPNAME_LASTMODIFIED.
     *
     * @param row Map representing database row
     * @param hostname connector host name
     * @param dbName database name
     * @param docId document id of DB doc
     * @param isContentFeed true if Feed type is content feed
     */
    private static void setOptionalProperties(Map<String, Object> row,
            JsonObjectUtil jsonObjectUtil, DBContext dbContext) {
        if (dbContext == null) {
            return;
        }
        // set Document Title
        Object docTitle = row.get(dbContext.getDocumentTitle());
        if (docTitle != null) {
            jsonObjectUtil.setProperty(SpiConstants.PROPNAME_TITLE, docTitle.toString());

        }
        // set last modified date
        Object lastModified = row.get(dbContext.getLastModifiedDate());
        if (lastModified != null && (lastModified instanceof Timestamp)) {
            jsonObjectUtil.setLastModifiedDate(SpiConstants.PROPNAME_LASTMODIFIED, (Timestamp) lastModified);

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
     * @return JsOnDocument
     * @throws DBException
     */
    public static JsonDocument generateMetadataURLFeed(String dbName,
            String[] primaryKeys, Map<String, Object> row, String hostname,
            DBContext dbContext, String type, TraversalContext context)
			throws DBException {

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
        String finalURL = "";
        if (isWithBaseURL) {
            baseURL = dbContext.getBaseURL();
            docIdColumn = dbContext.getDocumentIdField();
            Object docId = row.get(docIdColumn);
            /*
             * build final document URL if docId is not null. Send null
             * JsonDocument if document id is null.
             */
            if (docId != null) {
                finalURL = baseURL.trim() + docId.toString();
            } else {
                return null;
            }
            skipColumns.add(dbContext.getDocumentIdField());
        } else {
            skipColumns.add(dbContext.getDocumentURLField());
            Object docURL = row.get(dbContext.getDocumentURLField());
            if (docURL != null) {
                finalURL = row.get(dbContext.getDocumentURLField()).toString();
            } else {
                return null;
            }
        }

        JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
        // get doc id from primary key values
        String docId = DocIdUtil.generateDocId(primaryKeys, row);

        String xmlRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, null, dbContext, true);

        /*
         * This method add addition database columns(last modified and doc
         * title) which needs to skip while sending as metadata as they are
         * already consider as metadata.
         */
        skipOtherProperties(skipColumns, dbContext);

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_SEARCHURL, finalURL);

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, finalURL);


        // Set feed type as metadata_url
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.WEB.toString());

        // set doc id
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);


        jsonObjectUtil.setProperty(ROW_CHECKSUM, getChecksum(xmlRow.getBytes()));

        /*
         * set action as add. Even when contents are updated the we still we set
         * action as add and GSA overrides the old copy with new updated one.
         * Hence ADD action is applicable to both add and update
         */
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());


        /*
         * Set other doc properties like Last Modified date and document title.
         */
        setOptionalProperties(row, jsonObjectUtil, dbContext);
        skipColumns.addAll(Arrays.asList(primaryKeys));
        setMetaInfo(jsonObjectUtil, row, skipColumns);

        JsonDocument jsonDocument = new JsonDocument(
                jsonObjectUtil.getJsonObject());
        return jsonDocument;
    }

    /**
     * This method will add value of each column as metadata to Database
     * document except the values of columns in skipColumns list.
     *
     * @param doc
     * @param row
     * @param skipColumns list of columns needs to ignore while indexing
     */
    private static void setMetaInfo(JsonObjectUtil jsonObjectUtil,
            Map<String, Object> row, List<String> skipColumns) {
        // get all column names as key set
        Set<String> keySet = row.keySet();
        for (String key : keySet) {
            // set column value as metadata and column name as meta-name.
            if (!skipColumns.contains(key)) {
                Object value = row.get(key);
                if (value != null)
                    jsonObjectUtil.setProperty(key, value.toString());

            } else {
                LOG.info("skipping metadata indexing of column " + key);
            }
        }
    }

    /**
     * This method converts Large Object(BLOB or CLOB) into equivalent
     * JsonDocument.
     *
     * @param dbName
     * @param primaryKeys
     * @param row
     * @param hostname
     * @param largeObjectField
     * @return JsonDocument for BLOB/CLOB data
     * @throws DBException
     */
    public static JsonDocument largeObjectToDoc(String dbName,
            String[] primaryKeys, Map<String, Object> row, String hostname,
            DBContext dbContext, TraversalContext context) throws DBException {

        // get doc id from primary key values
        String docId = DocIdUtil.generateDocId(primaryKeys, row);

        String clobValue = null;
        JsonDocument jsonDocument;
        JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

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
         * check if large object data value for null.If large object is null,
         * then don't set content else handle the content as per LOB type.
         */
        if (largeObject != null) {
            /*
             * check if large object is of type BLOB from the the type of
             * largeObject. If the largeObject is of type java.sql.Blob or byte
             * array means large object is of type BLOB else it is CLOB.
             */
            byte[] blobContent = null;
            // Maximum document size that connector manager supports.
            long maxDocSize = context.maxDocumentSize();

            if (largeObject instanceof byte[]) {
                blobContent = (byte[]) largeObject;
                int length = blobContent.length;
                /*
                 * Check if the size of document exceeds Max document size that
                 * Connector manager supports. Skip document if it exceeds.
                 */
                if (length > maxDocSize) {
                    LOG.warning("Size of the document '" + docId
                            + "' is larger than supported");
                    return null;
                }

                /*
                 * If skipped document exception occurs while setting BLOB
                 * content means mime type or content encoding of the current
                 * document is not supported.
                 */

                jsonObjectUtil = setBlobContent(blobContent, jsonObjectUtil, dbName, row, dbContext, primaryKeys, context, docId);
				if (jsonObjectUtil == null) {
					// Return null if the mimetype not supported for the
					// document
					return null;
				}

            } else if (largeObject instanceof Blob) {
                int length;

                try {
                    length = (int) ((Blob) largeObject).length();
                    /*
                     * Check if the size of document exceeds Max document size
                     * that Connector manager supports. Skip document if it
                     * exceeds.
                     */
                    if (length > maxDocSize) {
                        LOG.warning("Size of the document '" + docId
                                + "' is larger than supported");
                        return null;
                    }
                    blobContent = ((Blob) largeObject).getBytes(0, length);
                } catch (SQLException e) {
                    // try to get byte array of blob content from input
                    // stream
                    InputStream contentStream;
                    try {
                        length = (int) ((Blob) largeObject).length();
                        /*
                         * Check if the size of document exceeds Max document
                         * size that Connector manager supports. Skip document
                         * if it exceeds.
                         */
                        if (length > maxDocSize) {
                            LOG.warning("Size of the document '" + docId
                                    + "' is larger than supported");
                            return null;
                        }
                        contentStream = ((Blob) largeObject).getBinaryStream();
                        if (contentStream != null) {
                            blobContent = getBytes(length, contentStream);
                        }

                    } catch (SQLException e1) {
                        LOG.warning("Exception occured while retrivieving Blob content:\n"
                                + e.toString());
                        return null;
                    }
                }
                jsonObjectUtil = setBlobContent(blobContent, jsonObjectUtil, dbName, row, dbContext, primaryKeys, context, docId);
				if (jsonObjectUtil == null) {
					// Return null if the mimetype not supported for the
					// document
					return null;
				}
            } else {
                /*
                 * get the value of CLOB as StringBuilder. iBATIS returns char
                 * array or String for CLOB data depending upon Database.
                 */
                if (largeObject instanceof char[]) {
                    int length = ((char[]) largeObject).length;
                    /*
                     * Check if the size of document exceeds Max document size
                     * that Connector manager supports. Skip document if it
                     * exceeds.
                     */
                    if (length > maxDocSize) {
                        LOG.warning("Size of the document '" + docId
                                + "' is larger than supported");
                        return null;
                    }
                    clobValue = new String((char[]) largeObject);
                } else if (largeObject instanceof String) {
                    int length = largeObject.toString().getBytes().length;
                    /*
                     * Check if the size of document exceeds Max document size
                     * that Connector manager supports. Skip document if it
                     * exceeds.
                     */
                    if (length > maxDocSize) {
                        LOG.warning("Size of the document '" + docId
                                + "' is larger than supported");
                        return null;
                    }
                    clobValue = largeObject.toString();
                } else if (largeObject instanceof Clob) {

                    try {
                        int length = (int) ((Clob) largeObject).length();
                        /*
                         * Check if the size of document exceeds Max document
                         * size that Connector manager supports. Skip document
                         * if it exceeds.
                         */
                        if (length > maxDocSize) {
                            LOG.warning("Size of the document '" + docId
                                    + "' is larger than supported");
                            return null;
                        }
                        InputStream clobStream = ((Clob) largeObject).getAsciiStream();
                        clobValue = new String(getBytes(length, clobStream));
                    } catch (SQLException e) {
                        LOG.warning("Exception occured while retrivieving Clob content:\n"
                                + e.toString());
                    }
                }
                if (clobValue != null) {

                    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT, clobValue.toString());

                } else {
                    LOG.warning("Content of documnet " + docId + " is null");
                    return null;
                }
                jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);


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
                jsonObjectUtil.setProperty(ROW_CHECKSUM, docCheckSum);

                LOG.info("CLOB Data found");
            }
            /*
             * If large object is null then send empty value.
             */
        } else {
            /* get xml representation of document(exclude the LOB column). */
            Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
            String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);

            // get checksum of columns other than LOB.
            String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());

            // set checksum for this document
            jsonObjectUtil.setProperty(ROW_CHECKSUM, otherColumnCheckSum);

            LOG.warning("Content of Document " + docId + " has null value.");
        }

        // set doc id
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);


        // set feed type as content feed
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.CONTENT.toString());
        // set action as add
        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());


        // set "ispublic" false if authZ query is provided by the user.
        if (dbContext != null && !dbContext.isPublicFeed()) {
            jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");

        }

        /*
         * if connector admin has has provided Fetch URL column the use the
         * value of this column as a "Display URL". Else construct the display
         * URL and use it.
         */
        Object displayURL = row.get(dbContext.getFetchURLField());
        if (displayURL != null && displayURL.toString().trim().length() > 0) {
            jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, displayURL.toString().trim());

            skipColumns.add(displayURL.toString());
        } else {
            jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, getDisplayUrl(hostname, dbName, docId));

        }

        skipOtherProperties(skipColumns, dbContext);
        skipColumns.addAll(Arrays.asList(primaryKeys));

        setOptionalProperties(row, jsonObjectUtil, dbContext);

        setMetaInfo(jsonObjectUtil, row, skipColumns);

        /*
         * Set other doc properties
         */
		jsonDocument = new JsonDocument(jsonObjectUtil.getJsonObject());
		return jsonDocument;
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

    /**
     * This method converts the Input AStream into byte array.
     *
     * @param length
     * @param inStream
     * @return byte array of Input Stream
     */
    private static byte[] getBytes(int length, InputStream inStream) {

        int bytesRead = 0;
        byte[] content = new byte[length];
        while (bytesRead < length) {
            int result;
            try {
                result = inStream.read(content, bytesRead, length - bytesRead);
                if (result == -1)
                    break;
                bytesRead += result;
            } catch (IOException e) {
                LOG.warning("Exception occurred while converting InputStream into byte array"
                        + e.toString());
                return null;
            }
        }
        return content;
    }

    /**
     * This method sets the content of blob data in JsonDocument.
     *
     * @param blobContent BLOB content to be set
     * @param dbName name of the database
     * @param row Map representing row in the database table
     * @param dbContext object of DBContext
     * @param primaryKeys primary key columns
     * @return JsonDocument
     * @throws DBException
     */
    private static JsonObjectUtil setBlobContent(byte[] blobContent,
            JsonObjectUtil jsonObjectUtil, String dbName,
            Map<String, Object> row, DBContext dbContext, String[] primaryKeys,
			TraversalContext context, String docId) throws DBException {
        /*
         * First try to get the MIME type of this file from the result set. If
         * it does not maintain a column for MIME type try to get the MIME type
         * of this document(file stored as BLOB in database) using MIME type
         * finder utility.
         */
        String mimeType = "";
        mimeType = new MimeTypeFinder().find(blobContent, context);

        // get the mime type supported.


        // set mime type for this document

        jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, mimeType);


        // Set content
        int mimeTypeSupportLevel = context.mimeTypeSupportLevel(mimeType);
        if (mimeTypeSupportLevel == 0) {
            jsonObjectUtil.setBinaryContent(SpiConstants.PROPNAME_CONTENT, null);
			LOG.info("Setting Contents null beacuse MimeType" + mimeType
					+ " not supported for Document with id " + docId);
		} else if (mimeTypeSupportLevel < 0) {
			LOG.info("Skipping Document beacuse MimeType " + mimeType
					+ "is excluded for Document with Id: " + docId);
			// Return null if mimetype for the document is not supported
			return null;
        } else {
            jsonObjectUtil.setBinaryContent(SpiConstants.PROPNAME_CONTENT, blobContent);
        }
        // get xml representation of document(exclude the BLOB column).
        Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
        String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);
        // get checksum of blob
        String blobCheckSum = Util.getChecksum((blobContent));
        // get checksum of other column
        String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());
        // get checksum of blob object and other column
        String docCheckSum = Util.getChecksum((otherColumnCheckSum + blobCheckSum).getBytes());
        // set checksum of this document
        jsonObjectUtil.setProperty(ROW_CHECKSUM, docCheckSum);
        LOG.info("BLOB Data found");

        return jsonObjectUtil;
    }
}
