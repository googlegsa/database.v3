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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
public class UrlDocumentBuilder extends JsonDocumentUtil {
  private static final Logger LOG =
      Logger.getLogger(UrlDocumentBuilder.class.getName());

  private final String type;

  public UrlDocumentBuilder(DBContext dbContext, String type) {
    super(dbContext);

    this.type = type;
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
  public JsonDocument fromRow(Map<String, Object> row) throws DBException {
    boolean isWithBaseURL = type.equalsIgnoreCase(JsonDocumentUtil.WITH_BASE_URL);

    /*
     * skipColumns maintain the list of column which needs to skip while
     * indexing as they are not part of metadata or they already considered for
     * indexing. For example document_id column, MIME type column, URL columns.
     */
    List<String> skipColumns = new ArrayList<String>();

    String finalURL;
    if (isWithBaseURL) {
      String docIdColumn = dbContext.getDocumentIdField();
      Object docId = row.get(docIdColumn);

      // Build final document URL if docId is not null. Send null JsonDocument
      // if document ID is null.
      if (docId != null) {
        String baseURL = dbContext.getBaseURL();
        finalURL = baseURL.trim() + docId.toString();
      } else {
        return null;
      }
      skipColumns.add(dbContext.getDocumentIdField());
    } else {
      Object docURL = row.get(dbContext.getDocumentURLField());
      if (docURL != null) {
        finalURL = row.get(dbContext.getDocumentURLField()).toString();
      } else {
        return null;
      }
      skipColumns.add(dbContext.getDocumentURLField());
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

    return new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
  }
}
