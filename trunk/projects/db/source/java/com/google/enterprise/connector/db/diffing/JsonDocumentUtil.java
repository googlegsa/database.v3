// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db.diffing;

import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;

import java.io.InputStream;
import java.io.CharArrayReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
public class JsonDocumentUtil {
  private static final Logger LOG =
      Logger.getLogger(JsonDocumentUtil.class.getName());
  private static final String MIMETYPE = "text/html";
  public static final String NO_TIMESTAMP = "NO_TIMESTAMP";
  public static final String NO_DOCID = "NO_DOCID";
  public static final String PRIMARY_KEYS_SEPARATOR = ",";
  public static final String ROW_CHECKSUM = "google:sum";
  public static final String WITH_BASE_URL = "withBaseURL";

  // This class should not be initialized.
  private JsonDocumentUtil() {
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
      DBContext dbContext) throws DBException {

    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    String contentXMLRow =
        XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT, contentXMLRow);
    String docId = DocIdUtil.generateDocId(primaryKeys, row);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

    // TODO: Look into which encoding/charset to use for getBytes().
    String completeXMLRow =
        XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, true);
    jsonObjectUtil.setProperty(ROW_CHECKSUM,
                               Util.getChecksum(completeXMLRow.getBytes()));

    // Set "ispublic" false if authZ query is provided by the user.
    if (dbContext != null && !dbContext.isPublicFeed()) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    }

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                               Util.getDisplayUrl(hostname, dbName, docId));

    // Set feed type as content feed.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.CONTENT.toString());

    // Set other doc properties.
    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);
    JsonDocument jsonDoc = new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
    return jsonDoc;
  }

  /**
   * Converts the given row into the equivalent Metadata-URL feed document.
   * There could be two scenarios depending upon how we get the URL of document.
   * In first scenario one of the column hold the complete URL of the document
   * and other columns holds the metadata of primary document. The name of URL
   * column is provided by user in configuration form. In second scenario the
   * URL of primary document is build by concatenating the base url and document
   * ID. COnnector admin provides the Base URL and document ID column in DB
   * connector configuration form.
   *
   * @param dbName Name of database
   * @param primaryKeys array of primary key columns
   * @param row map representing database row.
   * @param hostname fully qualified connector hostname.
   * @param dbContext instance of DBContext.
   * @param type represent how to get URL of the document. If value is
   *          "withBaseURL" it means we have to build document URL using base
   *          URL and document ID.
   * @return JsOnDocument
   * @throws DBException
   */
  public static JsonDocument generateMetadataURLFeed(String dbName,
      String[] primaryKeys, Map<String, Object> row, String hostname,
      DBContext dbContext, String type) throws DBException {

    boolean isWithBaseURL = type.equalsIgnoreCase(JsonDocumentUtil.WITH_BASE_URL);

    /*
     * skipColumns maintain the list of column which needs to skip while
     * indexing as they are not part of metadata or they already considered for
     * indexing. For example document_id column, MIME type column, URL columns.
     */
    List<String> skipColumns = new ArrayList<String>();

    String baseURL = null;
    String docIdColumn = null;
    String finalURL = "";
    if (isWithBaseURL) {
      baseURL = dbContext.getBaseURL();
      docIdColumn = dbContext.getDocumentIdField();
      Object docId = row.get(docIdColumn);

      // Build final document URL if docId is not null. Send null JsonDocument
      // if document ID is null.
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

    // Get doc ID from primary key values.
    String docId = DocIdUtil.generateDocId(primaryKeys, row);
    String xmlRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, null,
                                       dbContext, true);
    // This method adds addition database columns (last modified and doc title)
    // which needs to skip while sending as metadata as they are already
    // considered as metadata.
    Util.skipOtherProperties(skipColumns, dbContext);

    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_SEARCHURL, finalURL);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, finalURL);

    // Set feed type as metadata_url.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.WEB.toString());
    // Set doc ID.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);
    jsonObjectUtil.setProperty(ROW_CHECKSUM,
                               Util.getChecksum(xmlRow.getBytes()));
    // Set action as add. Even when contents are updated the we still we set
    // action as add and GSA overrides the old copy with new updated one.
    // Hence ADD action is applicable to both add and update.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());
    // Set other doc properties like Last Modified date and document title.
    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);
    skipColumns.addAll(Arrays.asList(primaryKeys));
    Util.setMetaInfo(jsonObjectUtil, row, skipColumns);

    JsonDocument jsonDocument = new JsonDocument(
        jsonObjectUtil.getProperties(), jsonObjectUtil.getJsonObject());
    return jsonDocument;
  }

  /**
   * Converts a large Object (BLOB or CLOB) into equivalent JsonDocument.
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

    // Get doc ID from primary key values.
    String docId = DocIdUtil.generateDocId(primaryKeys, row);
    String clobValue = null;
    JsonDocument jsonDocument;
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    // skipColumns maintain the list of column which needs to skip while
    // indexing as they are not part of metadata or they already considered for
    // indexing. For example document_id column, MIME type column, URL columns.
    List<String> skipColumns = new ArrayList<String>();

    // Get the value of large object from map representing a row.
    Object largeObject = row.get(dbContext.getLobField());
    skipColumns.add(dbContext.getLobField());

    // Check if large object data value for null.  If large object is null,
    // then do not set content, else handle the content as per LOB type.
    if (largeObject != null) {
      // Check if large object is of type BLOB from the the type of largeObject.
      // If the largeObject is of type java.sql.Blob or byte array means large
      // object is of type BLOB, else it is CLOB.
      byte[] binaryContent = null;
      // Maximum document size that connector manager supports.
      long maxDocSize = context.maxDocumentSize();

      if (largeObject instanceof byte[]) {
        binaryContent = (byte[]) largeObject;
        int length = binaryContent.length;
        // Check if the size of document exceeds Max document size that
        // Connector Manager supports. Skip document if it exceeds.
        if (length > maxDocSize) {
          LOG.warning("Size of the document '" + docId
              + "' is larger than supported");
          return null;
        }

        setBinaryContent(binaryContent, jsonObjectUtil,
            dbName, row, dbContext, primaryKeys);
        LOG.info("BLOB Data found");
      } else if (largeObject instanceof Blob) {
        int length;
        try {
          length = (int) ((Blob) largeObject).length();
          // Check if the size of document exceeds Max document size that
          // Connector Manager supports. Skip document if it exceeds.
          if (length > maxDocSize) {
            LOG.warning("Size of the document '" + docId
                + "' is larger than supported");
            return null;
          }
        } catch (SQLException e) {
          LOG.warning("Exception occurred while retrieving Blob content length:"
              + "\n" + e.toString());
          return null;
        }

        try {
          binaryContent = ((Blob) largeObject).getBytes(1, length);
        } catch (SQLException e) {
          // Try to get byte array of blob content from input stream.
          InputStream contentStream;
          try {
            contentStream = ((Blob) largeObject).getBinaryStream();
            if (contentStream != null) {
              binaryContent = Util.getBytes(length, contentStream);
            }
          } catch (SQLException e1) {
            LOG.warning("Exception occurred while retrieving Blob content:\n"
                + e.toString());
            return null;
          }
        }
        setBinaryContent(binaryContent, jsonObjectUtil,
            dbName, row, dbContext, primaryKeys);
        LOG.info("BLOB Data found");
      } else {
        // Get the value of CLOB as StringBuilder. iBATIS returns char array or
        // String for CLOB data depending upon Database.
        int length;
        Reader clobReader;
        if (largeObject instanceof char[]) {
          length = ((char[]) largeObject).length;
          clobReader = new CharArrayReader((char[]) largeObject);
        } else if (largeObject instanceof String) {
          length = ((String) largeObject).length();
          clobReader = new StringReader((String) largeObject);
        } else if (largeObject instanceof Clob) {
          try {
            length = (int) ((Clob) largeObject).length();
            clobReader = ((Clob) largeObject).getCharacterStream();
          } catch (SQLException e) {
            LOG.warning("Exception occurred while retrieving Clob content:\n"
                        + e.toString());
            return null;
          }
        } else {
          length = 0;
          clobReader = null;
        }

        if (clobReader != null && length <= maxDocSize) {
          binaryContent = Util.getBytes(length, clobReader);
          length = binaryContent.length;
        } else {
          // No content or content obviously too large.
        }

        // Check if the size of document exceeds Max document size that
        // Connector Manager supports. Skip document if it exceeds.
        if (length > maxDocSize) {
          LOG.warning("Size of the document '" + docId
                      + "' is larger than supported");
          return null;
        }

        setBinaryContent(binaryContent, jsonObjectUtil,
            dbName, row, dbContext, primaryKeys);
        LOG.info("CLOB Data found");
      }
      /*
       * If large object is null then send empty value.
       */
    } else {
      // Get XML representation of document(exclude the LOB column).
      Map<String, Object> rowForXmlDoc = Util.getRowForXmlDoc(row, dbContext);
      String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "",
                                         dbContext, true);
      // Get checksum of columns other than LOB.
      String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());

      // Set checksum for this document.
      jsonObjectUtil.setProperty(ROW_CHECKSUM, otherColumnCheckSum);

      LOG.warning("Content of Document " + docId + " has null value.");
    }

    // Set doc id.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);

    // Set feed type as content feed.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.CONTENT.toString());
    // Set action as add.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

    // Set "ispublic" false if authZ query is provided by the user.
    if (dbContext != null && !dbContext.isPublicFeed()) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    }

    // If connector admin has provided Fetch URL column then use the value of
    // that column as a "Display URL". Else construct the display URL.
    Object displayURL = row.get(dbContext.getFetchURLField());
    if (displayURL != null && displayURL.toString().trim().length() > 0) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                                 displayURL.toString().trim());
      skipColumns.add(displayURL.toString());
    } else {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                                 Util.getDisplayUrl(hostname, dbName, docId));
    }

    Util.skipOtherProperties(skipColumns, dbContext);
    skipColumns.addAll(Arrays.asList(primaryKeys));
    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);
    Util.setMetaInfo(jsonObjectUtil, row, skipColumns);

    // Set other doc properties.
    jsonDocument = new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
    return jsonDocument;
  }

  /**
   * Sets the content of LOB data in JsonDocument.
   *
   * @param binaryContent LOB content to be set
   * @param dbName name of the database
   * @param row Map representing row in the database table
   * @param dbContext object of DBContext
   * @param primaryKeys primary key columns
   * @throws DBException
   */
  public static void setBinaryContent(byte[] binaryContent,
      JsonObjectUtil jsonObjectUtil, String dbName, Map<String, Object> row,
      DBContext dbContext, String[] primaryKeys)
      throws DBException {
    String mimeType =
        dbContext.getMimeTypeDetector().getMimeType(null, binaryContent);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, mimeType);

    // TODO (bmj): I would really like to skip caching the content if the
    // mimeTypeSupportLevel is <= 0, but I don't have a TraversalContext here.
    jsonObjectUtil.setBinaryContent(SpiConstants.PROPNAME_CONTENT, binaryContent);

    // Get XML representation of document (exclude the LOB column).
    Map<String, Object> rowForXmlDoc = Util.getRowForXmlDoc(row, dbContext);
    String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys,
                                       "", dbContext, true);

    // Get checksum of LOB object and other columns.
    String docCheckSum = Util.getChecksum(xmlRow.getBytes(), binaryContent);

    // Set checksum of this document.
    jsonObjectUtil.setProperty(ROW_CHECKSUM, docCheckSum);
  }
}
