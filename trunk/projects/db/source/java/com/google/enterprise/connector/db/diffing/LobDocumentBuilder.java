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
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.spi.Value;

import java.io.InputStream;
import java.io.CharArrayReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
class LobDocumentBuilder extends DocumentBuilder {
  private static final Logger LOG =
      Logger.getLogger(LobDocumentBuilder.class.getName());

  private final TraversalContext context;

  protected LobDocumentBuilder(DBContext dbContext, TraversalContext context) {
    super(dbContext);

    this.context = context;
  }

  private byte[] getBinaryContent(Object largeObject, String docId)
      throws SQLException {
    // Check if large object is of type BLOB from the the type of largeObject.
    // If the largeObject is of type java.sql.Blob or byte array means large
    // object is of type BLOB, else it is CLOB.
    byte[] binaryContent;
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

      LOG.info("BLOB Data found");
    } else if (largeObject instanceof Blob) {
      int length = (int) ((Blob) largeObject).length();
      // Check if the size of document exceeds Max document size that
      // Connector Manager supports. Skip document if it exceeds.
      if (length > maxDocSize) {
        LOG.warning("Size of the document '" + docId
            + "' is larger than supported");
        return null;
      }

      try {
        binaryContent = ((Blob) largeObject).getBytes(1, length);
      } catch (SQLException e) {
        // Try to get byte array of blob content from input stream.
        InputStream contentStream = ((Blob) largeObject).getBinaryStream();
        if (contentStream != null) {
          binaryContent = Util.getBytes(length, contentStream);
        } else {
          binaryContent = null;
        }
      }
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
        length = (int) ((Clob) largeObject).length();
        clobReader = ((Clob) largeObject).getCharacterStream();
      } else {
        // It's not a CLOB, but we'll include it anyway.
        String value = largeObject.toString();
        length = value.length();
        clobReader = new StringReader(value);
      }

      if (clobReader != null && length <= maxDocSize) {
        binaryContent = Util.getBytes(length, clobReader);
        length = binaryContent.length;
      } else {
        // No content or content obviously too large.
        binaryContent = null;
      }

      // Check if the size of document exceeds Max document size that
      // Connector Manager supports. Skip document if it exceeds.
      if (length > maxDocSize) {
        LOG.warning("Size of the document '" + docId
                    + "' is larger than supported");
        return null;
      }

      LOG.info("CLOB Data found");
    }
    return binaryContent;
  }

  private String getChecksum(
      Map<String, Object> row) throws DBException {
    // Get XML representation of document(exclude the LOB column).
    Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row);
    return super.getChecksum(rowForXmlDoc, "");
  }

  @Override
  protected ContentHolder getContentHolder(Map<String, Object> row,
      String docId) throws DBException {
    // Get the value of large object from map representing a row.
    Object largeObject = row.get(dbContext.getLobField());

    // Check if large object data value for null.  If large object is null,
    // then do not set content, else handle the content as per LOB type.
    byte[] binaryContent;
    if (largeObject == null) {
      binaryContent = null;
    } else {
      try {
        binaryContent = getBinaryContent(largeObject, docId);
      } catch (SQLException e) {
        throw new DBException("Error retrieving LOB content", e);
      }
    }
    if (binaryContent != null) {
      return getContentHolder(binaryContent, row);
    } else {
      LOG.warning("Content of Document " + docId + " has null value.");
      return new ContentHolder(null, getChecksum(row), null);
    }
  }

  /**
   * Converts a large Object (BLOB or CLOB) into equivalent JsonDocument.
   */
  @Override
  protected JsonDocument getJsonDocument(DocumentHolder holder)
      throws DBException {
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    List<String> skipColumns = new ArrayList<String>();

    skipColumns.add(dbContext.getLobField());

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, holder.docId);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.CONTENT.toString());
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

    // Set "ispublic" false if authZ query is provided by the user.
    if (dbContext != null && !dbContext.isPublicFeed()) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    }

    jsonObjectUtil.setBinaryContent(SpiConstants.PROPNAME_CONTENT,
        (Value) holder.contentHolder.content);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE,
        holder.contentHolder.mimeType);

    // If connector admin has provided Fetch URL column then use the value of
    // that column as a "Display URL". Else construct the display URL.
    // TODO(jlacey): What happens if getFetchURLField returns empty or null?
    Object displayURL = holder.row.get(dbContext.getFetchURLField());
    if (displayURL != null && displayURL.toString().trim().length() > 0) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                                 displayURL.toString().trim());
      // TODO(jlacey): This is broken, isn't it, confusing the field
      // to skip with the displayURL value?
      skipColumns.add(displayURL.toString());
    } else {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                                 getDisplayUrl(hostname, dbName, holder.docId));
    }

    skipLastModified(skipColumns, dbContext);
    skipColumns.addAll(Arrays.asList(primaryKeys));
    setLastModified(holder.row, jsonObjectUtil, dbContext);
    setMetaInfo(jsonObjectUtil, holder.row, skipColumns);

    return new JsonDocument(jsonObjectUtil.getProperties(),
     jsonObjectUtil.getJsonObject());
  }

  private ContentHolder getContentHolder(byte[] binaryContent,
      Map<String, Object> row) throws DBException {
    String mimeType =
        dbContext.getMimeTypeDetector().getMimeType(null, binaryContent);

    // TODO (bmj): I would really like to skip caching the content if the
    // mimeTypeSupportLevel is <= 0, but I don't have a TraversalContext here.

    Value content = JsonObjectUtil.getBinaryValue(binaryContent);
    return new ContentHolder(content, getChecksum(binaryContent, row),
        mimeType);
  }

  private String getChecksum(byte[] binaryContent, Map<String, Object> row)
      throws DBException {
    // Get XML representation of document (exclude the LOB column).
    Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row);
    String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys,
                                       "", dbContext, true);

    // Get checksum of LOB object and other columns.
    String docCheckSum = Util.getChecksum(xmlRow.getBytes(), binaryContent);
    return docCheckSum;
  }

  /**
   * Copies all elements from map representing a row except BLOB
   * column and return the resultant map.
   *
   * @param row
   * @return map representing a database table row.
   */
  private Map<String, Object> getRowForXmlDoc(Map<String, Object> row) {
    Set<String> keySet = row.keySet();
    Map<String, Object> map = new HashMap<String, Object>();
    for (String key : keySet) {
      if (!dbContext.getLobField().equals(key)) {
        map.put(key, row.get(key));
      }
    }
    return map;
  }
}
