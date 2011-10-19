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

import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.db.DBConnectorType;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;
import com.google.enterprise.connector.util.diffing.TraversalContextManager;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A class which gets rows from Database using (@link DBClient) and converts
 * them to (@link JsonDocument) using (@link Util). Provides a collection over
 * the JsonDocument.
 */
public class RepositoryHandler {
  private static final Logger LOG = Logger.getLogger(RepositoryHandler.class.getName());
  private DBClient dbClient;
  private TraversalContextManager traversalContextManager;
  private static TraversalContext traversalContext;
  private int cursorDB = 0;
  private Integer minValue = null;
  private Integer maxValue = null;

  // EXC_NORMAL represents that DB Connector is running in normal mode
  private static final int MODE_NORMAL = 1;

  // EXC_METADATA_URL represents that DB Connector is running for indexing
  // External Metadada
  private static final int MODE_METADATA_URL = 2;

  // EXC_BLOB represents that DB Connector is running for indexing BLOB
  // data
  private static final int MODE_METADATA_BASE_URL = 3;

  // EXC_CLOB represents that DB Connector is running for indexing CLOB
  // data
  private static final int MODE_BLOB_CLOB = 4;

  public TraversalContext getTraversalContext() {
    return traversalContext;
  }

  public void setTraversalContext(TraversalContext traversalContext) {
    RepositoryHandler.traversalContext = traversalContext;
  }

  // current execution mode
  private int currentExcMode = -1;

  public static RepositoryHandler makeRepositoryHandlerFromConfig(
      DBClient dbClient, TraversalContextManager traversalContextManager) {

    RepositoryHandler repositoryHandler = new RepositoryHandler();
    repositoryHandler.traversalContextManager = traversalContextManager;
    repositoryHandler.cursorDB = 0;
    repositoryHandler.dbClient = dbClient;
    return repositoryHandler;
  }

  /**
   * Returns CursorDB.
   */

  public int getCursorDB() {
    return cursorDB;
  }

  /**
   * Sets the CursorDb.
   */

  public void setCursorDB(int cursorDB) {
    this.cursorDB = cursorDB;
  }

  /**
   * Function for fetching Database rows and providing a collection over
   * JsonDocument.
   */
  public LinkedList<JsonDocument> executeQueryAndAddDocs()
      throws SnapshotRepositoryRuntimeException {
    LinkedList<JsonDocument> docList = new LinkedList<JsonDocument>();
    List<Map<String, Object>> rows = null;

    if (!dbClient.getDBContext().isParameterizedQueryFlag()) {

      try {
        rows = dbClient.executePartialQuery(cursorDB, dbClient.getDBContext().getNumberOfRows());
      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.info("Repository Unreachable."
            + "Setting CursorDB value to zero to start traversal from begining after recovery. ");
        setCursorDB(0);
        LOG.warning("Unable to connect to the database\n" + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "Unable to connect to the database\n ", e);
      }

      if (rows.size() == 0) {
        LOG.info("Crawl cycle of database "
            + dbClient.getDBContext().getDbName() + " is completed at: "
            + new Date() + "\nTotal " + getCursorDB()
            + " records are crawled during this crawl cycle");
        setCursorDB(0);
      } else {
        setCursorDB(getCursorDB() + rows.size());
      }

    } else {
      if (minValue == null) {
        setMinValue(dbClient.getDBContext().getMinValue());
        setMaxValue(minValue + dbClient.getDBContext().getNumberOfRows());
      }

      if (minValue > dbClient.getDBContext().getMaxValue()) {
        LOG.info("Database Crawl Cycle Completed."
            + "Resetting  minvalue and maxvalue to start traversal from begining.. ");
        setMinValue(dbClient.getDBContext().getMinValue());
        setMaxValue(minValue + dbClient.getDBContext().getNumberOfRows());
        return docList;
      }

      try {
        rows = dbClient.executeParameterizePartialQuery(minValue, maxValue);
        setMinValue(maxValue + 1);
        setMaxValue(minValue + dbClient.getDBContext().getNumberOfRows());

      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.info("Repository Unreachable."
            + "Resetting  minvalue and maxvalue to start traversal from begining after recovery. ");
        setMinValue(dbClient.getDBContext().getMinValue());
        setMaxValue(minValue + dbClient.getDBContext().getNumberOfRows());
        LOG.warning("Unable to connect to the database\n" + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "Unable to connect to the database\n ", e);
      }
    }

    if (traversalContext == null) {
      LOG.info("Setting Traversal Context");
      setTraversalContext(traversalContextManager.getTraversalContext());
      JsonDocument.setTraversalContext(traversalContextManager.getTraversalContext());
    }

    JsonDocument jsonDoc = null;
    if (rows != null && rows.size() > 0) {

      currentExcMode = getExecutionScenario(dbClient.getDBContext());
      String logMessage = getExcLogMessage(currentExcMode);
      LOG.info(logMessage);

      switch (currentExcMode) {

      // execute the connector for metadata-url feed
      case MODE_METADATA_URL:

        for (Map<String, Object> row : rows) {
          try {
            jsonDoc = JsonDocumentUtil.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getDBContext().getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), "");
          } catch (DBException e) {
            LOG.warning("Cannot convert datbase record to JsonDocument for record"
                + row + "\n" + e);
          }
          docList.add(jsonDoc);
        }
        break;

      // execute the connector for BLOB data
      case MODE_METADATA_BASE_URL:
        jsonDoc = null;
        for (Map<String, Object> row : rows) {
          try {
            jsonDoc = JsonDocumentUtil.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getDBContext().getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), Util.WITH_BASE_URL);
          } catch (DBException e) {
            LOG.warning("Cannot convert datbase record to JsonDocument for record"
                + row + "\n" + e);
          }
          docList.add(jsonDoc);
        }

        break;

      // execute the connector for CLOB data
      case MODE_BLOB_CLOB:
        jsonDoc = null;
        for (Map<String, Object> row : rows) {
          try {
            jsonDoc = JsonDocumentUtil.largeObjectToDoc(dbClient.getDBContext().getDbName(), dbClient.getDBContext().getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), traversalContext);
            if (jsonDoc != null) {
              docList.add(jsonDoc);
            }
          } catch (DBException e) {
            LOG.warning("Cannot convert datbase record to JsonDocument for record"
                + row + "\n" + e);
          }

        }

        break;

      // execute the connector in normal mode
      default:
        for (Map<String, Object> row : rows) {
          try {
            jsonDoc = JsonDocumentUtil.rowToDoc(dbClient.getDBContext().getDbName(), dbClient.getDBContext().getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext().getXslt(), dbClient.getDBContext());
            if (jsonDoc != null) {
              docList.add(jsonDoc);
            }
          } catch (DBException e) {
            LOG.warning("Cannot convert datbase record to JsonDocument for record"
                + row + "\n" + e);
          }

        }
        break;
      }
    }
    LOG.info(docList.size() + " document(s) to be fed to GSA" + " at time: "
        + new Date());
    return docList;
  }

  public Integer getMinValue() {
    return minValue;
  }

  public void setMinValue(Integer minValue) {
    this.minValue = minValue;
  }

  public Integer getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Integer maxValue) {
    if (this.maxValue != null) {
      if (maxValue > dbClient.getDBContext().getMaxValue()) {
        maxValue = dbClient.getDBContext().getMaxValue();
      }
    }

    this.maxValue = maxValue;
  }

  /**
   * this method will detect the execution mode from the column names(Normal,
   * CLOB, BLOB or External Metadata) of the DB Connector and returns the
   * integer value representing execution mode
   */

  private int getExecutionScenario(DBContext dbContext) {

    String extMetaType = dbContext.getExtMetadataType();
    String lobField = dbContext.getLobField();
    String docURLField = dbContext.getDocumentURLField();
    String docIdField = dbContext.getDocumentIdField();
    if (extMetaType != null && extMetaType.trim().length() > 0
        && !extMetaType.equals(DBConnectorType.NO_EXT_METADATA)) {
      if (extMetaType.equalsIgnoreCase(DBConnectorType.COMPLETE_URL)
          && (docURLField != null && docURLField.trim().length() > 0)) {
        return MODE_METADATA_URL;
      } else if (extMetaType.equalsIgnoreCase(DBConnectorType.DOC_ID)
          && (docIdField != null && docIdField.trim().length() > 0)) {
        return MODE_METADATA_BASE_URL;
      } else if (extMetaType.equalsIgnoreCase(DBConnectorType.BLOB_CLOB)
          && (lobField != null && lobField.trim().length() > 0)) {
        return MODE_BLOB_CLOB;
      } else {
        /*
         * Explicitly change the mode of execution as user may switch from
         * "External Metadata Feed" mode to "Content Feed(for text data)" mode.
         */
        dbContext.setExtMetadataType(DBConnectorType.NO_EXT_METADATA);
        return MODE_NORMAL;
      }
    } else {
      /*
       * Explicitly change the mode of execution as user may switch from
       * "External Metadata Feed" mode to "Content Feed(for text data)" mode.
       */
      dbContext.setExtMetadataType(DBConnectorType.NO_EXT_METADATA);
      return MODE_NORMAL;
    }
  }

  /**
   * this method return appropriate log message as per current execution mode.
   *
   * @param excMode current execution mode
   * @return
   */
  private static String getExcLogMessage(int excMode) {

    switch (excMode) {

    case MODE_METADATA_URL: {
      /*
       * execution mode: Externam Metadata feed using complete document URL
       */
      return " DB Connector is running in External Metadata feed mode with complete document URL";
    }
    case MODE_METADATA_BASE_URL: {
      /*
       * execution mode: Externam Metadata feed using Base URL and document Id
       */
      return " DB Connector is running in External Metadata feed mode with Base URL and document ID";
    }
    case MODE_BLOB_CLOB: {
      /*
       * execution mode: Content feed mode for BLOB/CLOB data.
       */
      return " DB Connector is running in Content Feed Mode for BLOB/CLOB data";
    }

    default: {
      /*
       * execution mode: Content feed mode for Text data.
       */return " DB Connector is running in content feed mode for text data";
    }
    }

  }

}
