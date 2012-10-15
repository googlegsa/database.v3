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

import com.google.enterprise.connector.db.DBConnectorType;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.db.XmlUtils;
import com.google.enterprise.connector.db.diffing.UrlDocumentBuilder.UrlType;
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
abstract class DocumentBuilder {
  private static final Logger LOG =
      Logger.getLogger(DocumentBuilder.class.getName());

  public static final String NO_TIMESTAMP = "NO_TIMESTAMP";
  public static final String NO_DOCID = "NO_DOCID";
  public static final String PRIMARY_KEYS_SEPARATOR = ",";
  public static final String ROW_CHECKSUM = "google:sum";
  public static final String WITH_BASE_URL = "withBaseURL";

  /* TODO: Move this method to Util? */
  private static boolean isNonBlank(String value) {
    return value != null && value.trim().length() > 0;
  }

  /**
   * Detect the execution mode from the column names(Normal,
   * CLOB, BLOB or External Metadata) of the DB Connector and returns the
   * integer value representing execution mode
   */
  public static DocumentBuilder getInstance(DBContext dbContext,
      TraversalContext traversalContext) {
    String extMetaType = dbContext.getExtMetadataType();
    if (isNonBlank(extMetaType)
        && !extMetaType.equals(DBConnectorType.NO_EXT_METADATA)) {
      if (extMetaType.equalsIgnoreCase(DBConnectorType.COMPLETE_URL)
          && isNonBlank(dbContext.getDocumentURLField())) {
        LOG.info("DB Connector is running in External Metadata feed mode with "
            + "complete document URL");
        return new UrlDocumentBuilder(dbContext, UrlType.COMPLETE_URL);
      } else if (extMetaType.equalsIgnoreCase(DBConnectorType.DOC_ID)
          && isNonBlank(dbContext.getDocumentIdField())) {
        LOG.info("DB Connector is running in External Metadata feed mode with "
            + "Base URL and document ID");
        return new UrlDocumentBuilder(dbContext, UrlType.BASE_URL);
      } else if (extMetaType.equalsIgnoreCase(DBConnectorType.BLOB_CLOB)
          && isNonBlank(dbContext.getLobField())) {
        LOG.info(
            "DB Connector is running in Content Feed Mode for BLOB/CLOB data");
        return new LobDocumentBuilder(dbContext, traversalContext);
      }
    }

    // No matches found above.
    // Explicitly change the mode of execution as user may switch from
    // "External Metadata Feed" mode to "Content Feed(for text data)" mode.
    dbContext.setExtMetadataType(DBConnectorType.NO_EXT_METADATA);
    LOG.info("DB Connector is running in content feed mode for text data");
    return new MetadataDocumentBuilder(dbContext);
  }

  protected final DBContext dbContext;

  protected final String dbName;
  protected final String[] primaryKeys;
  protected final String hostname;

  protected DocumentBuilder(DBContext dbContext) {
    this.dbContext = dbContext;

    this.dbName = dbContext.getDbName();
    // TODO: Split this on the way into DBContext?
    this.primaryKeys =
        dbContext.getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR);
    this.hostname = dbContext.getHostname();
  }

  /**
   * Converts a database row to a document.
   *
   * @param row row of a table.
   * @return a {@code JsonDocument} representation of the row
   * @throws DBException
   */
  public abstract JsonDocument fromRow(Map<String, Object> row)
      throws DBException;
}
