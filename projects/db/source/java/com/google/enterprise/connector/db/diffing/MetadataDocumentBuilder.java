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

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.spi.SpiConstants;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
@VisibleForTesting
public class MetadataDocumentBuilder extends DocumentBuilder {
  private static final Logger LOG =
      Logger.getLogger(MetadataDocumentBuilder.class.getName());

  private static final String MIMETYPE = "text/html";

  private final String xslt;

  @VisibleForTesting
  public MetadataDocumentBuilder(DBContext dbContext) {
    super(dbContext);

    this.xslt = dbContext.getXslt();
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
  public JsonDocument fromRow(Map<String, Object> row) throws DBException {
    String docId = DocIdUtil.generateDocId(primaryKeys, row);
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, docId);
    // TODO: Look into which encoding/charset to use for getBytes().
    String completeXMLRow =
        XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, true);
    jsonObjectUtil.setProperty(ROW_CHECKSUM,
                               Util.getChecksum(completeXMLRow.getBytes()));

    String contentXMLRow =
        XmlUtils.getXMLRow(dbName, row, primaryKeys, xslt, dbContext, false);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_CONTENT, contentXMLRow);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

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
    return new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
  }
}
