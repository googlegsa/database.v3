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
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.spi.SpiConstants;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
class MetadataDocumentBuilder extends DocumentBuilder {
  private static final Logger LOG =
      Logger.getLogger(MetadataDocumentBuilder.class.getName());

  private static final String MIMETYPE = "text/html";

  private final String xslt;

  protected MetadataDocumentBuilder(DBContext dbContext) {
    super(dbContext);

    this.xslt = dbContext.getXslt();
  }

  @Override
  protected ContentHolder getContentHolder(Map<String, Object> row,
      String docId) throws DBException {
    return new ContentHolder(row, getChecksum(row, xslt), MIMETYPE);
  }

  private String getContent(ContentHolder holder) throws DBException {
    @SuppressWarnings("unchecked") Map<String, Object> row =
        (Map<String, Object>) holder.content;
    return XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
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
  @Override
  protected JsonDocument getJsonDocument(DocumentHolder holder)
      throws DBException {
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, holder.docId);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT,
        getContent(holder.contentHolder));

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

    // Set "ispublic" false if authZ query is provided by the user.
    if (dbContext != null && !dbContext.isPublicFeed()) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    }

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE,
        holder.contentHolder.mimeType);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DISPLAYURL,
                               getDisplayUrl(hostname, dbName, holder.docId));

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.CONTENT.toString());

    setLastModified(holder.row, jsonObjectUtil, dbContext);

    return new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
  }
}
