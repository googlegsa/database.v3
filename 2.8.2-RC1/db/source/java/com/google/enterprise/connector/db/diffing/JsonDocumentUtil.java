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

package com.google.enterprise.connector.db.diffing;

import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;

import java.io.InputStream;
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
  public static final String NO_TIMESTAMP = "NO_TIMESTAMP";
  public static final String NO_DOCID = "NO_DOCID";
  private static final Logger LOG = Logger.getLogger(JsonDocumentUtil.class.getName());
  public static final String PRIMARY_KEYS_SEPARATOR = ",";
  private static final String MIMETYPE = "text/html";
  public static final String ROW_CHECKSUM = "dbconnector:checksum";

  public static String WITH_BASE_URL = "withBaseURL";

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
      Map<String, Object> row, String hostname, String xslt, DBContext dbContext)
      throws DBException {

    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    String contentXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT, contentXMLRow);
    String docId = DocIdUtil.generateDocId(primaryKeys, row);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());

    // TODO: Look into which encoding/charset to use for getBytes()
    String completeXMLRow = XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, true);
    jsonObjectUtil.setProperty(ROW_CHECKSUM, Util.getChecksum(completeXMLRow.getBytes()));

    // set "ispublic" false if authZ query is provided by the user.
    if (dbContext != null && !dbContext.isPublicFeed()) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");

    }

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, MIMETYPE);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, Util.getDisplayUrl(hostname, dbName, docId));

    // set feed type as content feed
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.CONTENT.toString());

    /*
     * Set other doc properties
     */
    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);
    JsonDocument jsonDoc = new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
    return jsonDoc;
  }

  /**
   * This method convert given row into equivalent Metadata-URL feed. There
   * could be two scenarios depending upon how we get the URL of document. In
   * first scenario one of the column hold the complete URL of the document and
   * other columns holds the metadata of primary document. The name of URL
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
      /*
       * build final document URL if docId is not null. Send null JsonDocument
       * if document id is null.
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
     * This method add addition database columns(last modified and doc title)
     * which needs to skip while sending as metadata as they are already
     * consider as metadata.
     */
    Util.skipOtherProperties(skipColumns, dbContext);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_SEARCHURL, finalURL);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, finalURL);

    // Set feed type as metadata_url
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE, SpiConstants.FeedType.WEB.toString());

    // set doc id
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);

    jsonObjectUtil.setProperty(ROW_CHECKSUM, Util.getChecksum(xmlRow.getBytes()));

    /*
     * set action as add. Even when contents are updated the we still we set
     * action as add and GSA overrides the old copy with new updated one. Hence
     * ADD action is applicable to both add and update
     */
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION, SpiConstants.ActionType.ADD.toString());

    /*
     * Set other doc properties like Last Modified date and document title.
     */
    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);
    skipColumns.addAll(Arrays.asList(primaryKeys));
    Util.setMetaInfo(jsonObjectUtil, row, skipColumns);

    JsonDocument jsonDocument = new JsonDocument(
        jsonObjectUtil.getProperties(), jsonObjectUtil.getJsonObject());
    return jsonDocument;
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
     * indexing as they are not part of metadata or they already considered for
     * indexing. For example document_id column, MIME type column, URL columns.
     */
    List<String> skipColumns = new ArrayList<String>();

    /*
     * get the value of large object from map representing a row
     */
    Object largeObject = row.get(dbContext.getLobField());
    skipColumns.add(dbContext.getLobField());

    /*
     * check if large object data value for null.If large object is null, then
     * don't set content else handle the content as per LOB type.
     */
    if (largeObject != null) {
      /*
       * check if large object is of type BLOB from the the type of largeObject.
       * If the largeObject is of type java.sql.Blob or byte array means large
       * object is of type BLOB else it is CLOB.
       */
      byte[] blobContent = null;
      // Maximum document size that connector manager supports.
      long maxDocSize = context.maxDocumentSize();

      if (largeObject instanceof byte[]) {
        blobContent = (byte[]) largeObject;

        byte[] blobData = new byte[blobContent.length];
        for (int i = 0; i < blobContent.length; i++)
          blobData[i] = blobContent[i];

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
         * If skipped document exception occurs while setting BLOB content means
         * mime type or content encoding of the current document is not
         * supported.
         */

        jsonObjectUtil = Util.setBlobContent(blobData, jsonObjectUtil, dbName, row, dbContext, primaryKeys, docId);
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
           * Check if the size of document exceeds Max document size that
           * Connector manager supports. Skip document if it exceeds.
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
             * Check if the size of document exceeds Max document size that
             * Connector manager supports. Skip document if it exceeds.
             */
            if (length > maxDocSize) {
              LOG.warning("Size of the document '" + docId
                  + "' is larger than supported");
              return null;
            }
            contentStream = ((Blob) largeObject).getBinaryStream();
            if (contentStream != null) {
              blobContent = Util.getBytes(length, contentStream);
            }

          } catch (SQLException e1) {
            LOG.warning("Exception occured while retrivieving Blob content:\n"
                + e.toString());
            return null;
          }
        }
        jsonObjectUtil = Util.setBlobContent(blobContent, jsonObjectUtil, dbName, row, dbContext, primaryKeys, docId);
        if (jsonObjectUtil == null) {
          // Return null if the mimetype not supported for the
          // document
          return null;
        }
      } else {
        /*
         * get the value of CLOB as StringBuilder. iBATIS returns char array or
         * String for CLOB data depending upon Database.
         */
        if (largeObject instanceof char[]) {
          int length = ((char[]) largeObject).length;
          /*
           * Check if the size of document exceeds Max document size that
           * Connector manager supports. Skip document if it exceeds.
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
           * Check if the size of document exceeds Max document size that
           * Connector manager supports. Skip document if it exceeds.
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
             * Check if the size of document exceeds Max document size that
             * Connector manager supports. Skip document if it exceeds.
             */
            if (length > maxDocSize) {
              LOG.warning("Size of the document '" + docId
                  + "' is larger than supported");
              return null;
            }
            InputStream clobStream = ((Clob) largeObject).getAsciiStream();
            clobValue = new String(Util.getBytes(length, clobStream));
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
        Map<String, Object> rowForXmlDoc = Util.getRowForXmlDoc(row, dbContext);
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
      Map<String, Object> rowForXmlDoc = Util.getRowForXmlDoc(row, dbContext);
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
     * if connector admin has has provided Fetch URL column the use the value of
     * this column as a "Display URL". Else construct the display URL and use
     * it.
     */
    Object displayURL = row.get(dbContext.getFetchURLField());
    if (displayURL != null && displayURL.toString().trim().length() > 0) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, displayURL.toString().trim());

      skipColumns.add(displayURL.toString());
    } else {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, Util.getDisplayUrl(hostname, dbName, docId));

    }

    Util.skipOtherProperties(skipColumns, dbContext);
    skipColumns.addAll(Arrays.asList(primaryKeys));

    Util.setOptionalProperties(row, jsonObjectUtil, dbContext);

    Util.setMetaInfo(jsonObjectUtil, row, skipColumns);

    /*
     * Set other doc properties
     */
    jsonDocument = new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
    return jsonDocument;
  }

}
