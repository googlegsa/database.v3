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

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.InputStreamFactories;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.InputStreamFactory;

import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    // We may support less, due to the constraints of Util.getBytes().
    long maxDocSize = Math.min(context.maxDocumentSize(), Integer.MAX_VALUE);

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
      long length = ((Blob) largeObject).length();
      // Check if the size of document exceeds Max document size that
      // Connector Manager supports. Skip document if it exceeds.
      if (length > maxDocSize) {
        LOG.warning("Size of the document '" + docId
            + "' is larger than supported");
        return null;
      }

      try {
        binaryContent = ((Blob) largeObject).getBytes(1, (int) length);
      } catch (SQLException e) {
        // Try to get byte array of blob content from input stream.
        InputStream contentStream = ((Blob) largeObject).getBinaryStream();
        if (contentStream != null) {
          binaryContent = Util.getBytes((int) length, contentStream);
        } else {
          binaryContent = null;
        }
      }
      LOG.info("BLOB Data found");
    } else {
      // Get the value of CLOB as StringBuilder. iBATIS returns char array or
      // String for CLOB data depending upon Database.
      long length;
      Reader clobReader;
      if (largeObject instanceof char[]) {
        length = ((char[]) largeObject).length;
        clobReader = new CharArrayReader((char[]) largeObject);
      } else if (largeObject instanceof String) {
        length = ((String) largeObject).length();
        clobReader = new StringReader((String) largeObject);
      } else if (largeObject instanceof Clob) {
        length = ((Clob) largeObject).length();
        clobReader = ((Clob) largeObject).getCharacterStream();
      } else {
        // It's not a CLOB, but we'll include it anyway.
        String value = largeObject.toString();
        length = value.length();
        clobReader = new StringReader(value);
      }

      if (clobReader != null && length <= maxDocSize) {
        binaryContent = Util.getBytes((int) length, clobReader);
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

  @Override
  protected ContentHolder getContentHolder(Map<String, Object> row,
      List<String> primaryKey, String docId) throws DBException {
    // Get the value of large object from map representing a row.
    Object largeObject = row.get(dbContext.getLobField());

    // Custom LOB TypeHandler creates a partitial ContentHolder.
    // Finish up calculating the checksum and return the ContentHolder.
    if (largeObject instanceof DigestContentHolder) {
      DigestContentHolder holder = (DigestContentHolder) largeObject;
      // TODO: Look into which encoding/charset to use for getBytes().
      holder.updateDigest(
          getXmlDoc(getRowForXmlDoc(row), primaryKey, "").getBytes());
      return holder;
    }

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
      // TODO (bmj): We should really skip caching the content if the
      // mimeTypeSupportLevel is <= 0.
      InputStreamFactory content = 
          InputStreamFactories.newInstance(binaryContent);
      DigestContentHolder holder = new DigestContentHolder(content,
          dbContext.getMimeTypeDetector().getMimeType(null, binaryContent));
      holder.updateDigest(binaryContent);
      // TODO: Look into which encoding/charset to use for getBytes().
      holder.updateDigest(
          getXmlDoc(getRowForXmlDoc(row), primaryKey, "").getBytes());
      return holder;
    } else {
      LOG.warning("Content of Document " + docId + " has null value.");
      return new ContentHolder(null,
          getChecksum(getRowForXmlDoc(row), primaryKey, ""), null);
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
        (InputStreamFactory) holder.contentHolder.getContent());
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE,
        holder.contentHolder.getMimeType());

    // If connector admin has provided Fetch URL column then use the value of
    // that column as a "Display URL". Else construct the display URL.
    String displayUrl;
    String fetchUrlField = dbContext.getFetchURLField();
    if (fetchUrlField != null) {
      Object fetchUrl = holder.row.get(fetchUrlField);
      if (fetchUrl != null && fetchUrl.toString().trim().length() > 0) {
        displayUrl = fetchUrl.toString().trim();
        skipColumns.add(fetchUrlField);
      } else {
        displayUrl = getDisplayUrl(holder.docId);
      }
    } else {
      displayUrl = getDisplayUrl(holder.docId);
    }
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL, displayUrl);

    skipLastModified(skipColumns, dbContext);
    skipColumns.addAll(holder.primaryKey);
    setLastModified(holder.row, jsonObjectUtil, dbContext);
    setMetaInfo(jsonObjectUtil, holder.row, skipColumns);

    return new JsonDocument(jsonObjectUtil.getProperties(),
     jsonObjectUtil.getJsonObject());
  }

  /**
   * Returns a filtered Map of the row with the LOB field filtered out.
   *
   * @param row
   * @return map representing a database table row.
   */
  private Map<String, Object> getRowForXmlDoc(Map<String, Object> row) {
    return Maps.filterKeys(row, new Predicate<String>() {
        public boolean apply(String key) {
          return !dbContext.getLobField().equals(key);
        }
      });
  }
}
