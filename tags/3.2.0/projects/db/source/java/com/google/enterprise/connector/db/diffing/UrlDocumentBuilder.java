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
import com.google.enterprise.connector.spi.SpiConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Class for transforming database row to JsonDocument.
 */
class UrlDocumentBuilder extends DocumentBuilder {
  private static final Logger LOG =
      Logger.getLogger(UrlDocumentBuilder.class.getName());

  protected enum UrlType { COMPLETE_URL, BASE_URL };

  private final UrlType type;

  protected UrlDocumentBuilder(DBContext dbContext, UrlType type) {
    super(dbContext);

    this.type = type;
  }

  @Override
  protected ContentHolder getContentHolder(Map<String, Object> row,
      List<String> primaryKey, String docId) throws DBException {
    return new ContentHolder(null, getChecksum(row, primaryKey, null), null);
  }

  private String getUrl(Map<String, Object> row, List<String> skipColumns) {
    String finalURL;
    switch (type) {
      case BASE_URL: {
        String docIdField = dbContext.getDocumentIdField();
        Object urlDocId = row.get(docIdField);

        // Build final document URL if urlDocId is not null. Send null
        // JsonDocument if document ID is null.
        if (urlDocId != null) {
          String baseURL = dbContext.getBaseURL();
          finalURL = baseURL.trim() + urlDocId.toString();
        } else {
          return null;
        }
        skipColumns.add(docIdField);
        break;
      }

      case COMPLETE_URL: {
        String docUrlField = dbContext.getDocumentURLField();
        Object docURL = row.get(docUrlField);
        if (docURL != null) {
          finalURL = docURL.toString();
        } else {
          return null;
        }
        skipColumns.add(docUrlField);
        break;
      }

      default:
        throw new AssertionError(type.toString());
    }
    return finalURL;
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
   * @param row map representing database row.
   */
  @Override
  protected JsonDocument getJsonDocument(DocumentHolder holder)
      throws DBException {
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();

    // We don't need the doc ID in a metadata-and-URL feed, but it's
    // useful for consistent logging in JsonDocument.
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, holder.docId);

    List<String> skipColumns = new ArrayList<String>();

    String finalURL = getUrl(holder.row, skipColumns);
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_SEARCHURL, finalURL);

    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_FEEDTYPE,
                               SpiConstants.FeedType.WEB.toString());
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ACTION,
                               SpiConstants.ActionType.ADD.toString());

    skipLastModified(skipColumns, dbContext);
    skipColumns.addAll(holder.primaryKey);
    setMetaInfo(jsonObjectUtil, holder.row, skipColumns);

    return new JsonDocument(jsonObjectUtil.getProperties(),
        jsonObjectUtil.getJsonObject());
  }
}
