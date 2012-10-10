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
public class LobDocumentBuilder extends JsonDocumentUtil {
  private static final Logger LOG =
      Logger.getLogger(LobDocumentBuilder.class.getName());

  private final TraversalContext context;

  public LobDocumentBuilder(DBContext dbContext, TraversalContext context) {
    super(dbContext);

    this.context = context;
  }

  /**
   * Converts a large Object (BLOB or CLOB) into equivalent JsonDocument.
   */
  @Override
  public JsonDocument fromRow(Map<String, Object> row) throws DBException {
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
    return new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
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
  private void setBinaryContent(byte[] binaryContent,
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
