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

import com.google.common.collect.Lists;
import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;
import com.google.enterprise.connector.util.diffing.TraversalContextManager;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class which gets rows from a database using (@link DBClient) and
 * converts them to {@link DocumentSnaphot}s using {@link DocumentBuilder).
 */
public class RepositoryHandler {
  private static final Logger LOG = Logger.getLogger(RepositoryHandler.class.getName());

  private DBContext dbContext;
  private DBClient dbClient;
  private TraversalContextManager traversalContextManager;
  private static TraversalContext traversalContext;
  private int cursorDB = 0;
  private Integer keyValue = null;
  private String primaryKeyColumn = null;

  public TraversalContext getTraversalContext() {
    return traversalContext;
  }

  public void setTraversalContext(TraversalContext traversalContext) {
    RepositoryHandler.traversalContext = traversalContext;
  }

  // current execution mode
  private int currentExcMode = -1;

  public static RepositoryHandler makeRepositoryHandlerFromConfig(
      DBContext dbContext, TraversalContextManager traversalContextManager) {
    RepositoryHandler repositoryHandler = new RepositoryHandler();
    repositoryHandler.traversalContextManager = traversalContextManager;
    repositoryHandler.cursorDB = 0;
    repositoryHandler.dbContext = dbContext;
    repositoryHandler.dbClient = dbContext.getClient();
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
   * If user enters primary key column name in different case in database
   * connector configuration form, we need to map primary key column name
   * entered by user with actual column name in query. Below block of code map
   * the primary key column name entered by user with actual column name in
   * result set(map).
   */
  private void setPrimaryKeyColumn(Set<String> keySet) {
    String keys[] =
        dbContext.getPrimaryKeys().split(Util.PRIMARY_KEYS_SEPARATOR);
    String primaryKey = keys[0];
    for (String key : keySet) {
      if (primaryKey.equalsIgnoreCase(key)) {
        primaryKey = key;
        break;
      }
    }
    primaryKeyColumn = primaryKey;
  }

  /**
   *Updates the keyValue with the highest order key.
   */
  public void updateKeyValue(List<Map<String, Object>> rows) {
    if (primaryKeyColumn == null) {
      Map<String, Object> singleRow = rows.iterator().next();
      Set<String> keySet = singleRow.keySet();
      setPrimaryKeyColumn(keySet);
    }
    for (Map<String, Object> row : rows) {
      String newKeyValueString = row.get(primaryKeyColumn).toString();
      Integer newKeyValue = Integer.parseInt(newKeyValueString);
      if (keyValue < newKeyValue) {
        keyValue = newKeyValue;
      }

    }
  }

  /**
   * Function for fetching database rows and providing a collection of
   * snapshots.
   */
  public List<DocumentSnapshot> executeQueryAndAddDocs()
      throws SnapshotRepositoryRuntimeException {
    List<Map<String, Object>> rows = null;

    if (!dbContext.isParameterizedQueryFlag()) {
      try {
        rows = dbClient.executePartialQuery(cursorDB,
            dbContext.getNumberOfRows());
      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.info("Repository Unreachable.  Setting CursorDB value to zero to "
            + "start traversal from begining after recovery.");
        setCursorDB(0);
        LOG.warning("Unable to connect to the database\n" + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "Unable to connect to the database\n ", e);
      }

      if (rows.size() == 0) {
        LOG.info("Crawl cycle of database "
            + dbContext.getDbName() + " is completed at: "
            + new Date() + "\nTotal " + getCursorDB()
            + " records are crawled during this crawl cycle");
        setCursorDB(0);
      } else {
        setCursorDB(getCursorDB() + rows.size());
      }
    } else {
      try {
        // Replace the keyValue with minValue to retrieve first set of
        // maxRows number of Records
        if (keyValue == null) {
          keyValue = dbContext.getMinValue();
        }
        rows = dbClient.executeParameterizePartialQuery(keyValue);
      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.info("Repository Unreachable. Resetting keyValue to minValue for "
            + "starting traversal from begining after recovery.");
        keyValue = dbContext.getMinValue();
        LOG.warning("Unable to connect to the database\n" + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "Unable to connect to the database\n", e);
      }
      if (rows.size() == 0) {
        LOG.info("No records returned for keyValue= " + keyValue);
        LOG.info("Crawl cycle completed for ordered Database. Resetting "
            + "keyValue to minValue for starting traversal from begining.");
        keyValue = dbContext.getMinValue();
      } else {
        updateKeyValue(rows);
      }
    }
    if (traversalContext == null) {
      LOG.info("Setting Traversal Context");
      setTraversalContext(traversalContextManager.getTraversalContext());
      JsonDocument.setTraversalContext(
          traversalContextManager.getTraversalContext());
    }

    return getDocList(rows);
  }

  private List<DocumentSnapshot> getDocList(List<Map<String, Object>> rows) {
    List<DocumentSnapshot> docList = Lists.newArrayList();
    if (rows != null && rows.size() > 0) {
      DocumentBuilder docBuilder =
          DocumentBuilder.getInstance(dbContext, traversalContext);

      for (Map<String, Object> row : rows) {
        try {
          DocumentSnapshot snapshot = docBuilder.getDocumentSnapshot(row);
          if (snapshot != null) {
            if (LOG.isLoggable(Level.FINER)) {
              LOG.finer("DBSnapshotRepository returns document with docID "
                  + DocIdUtil.decodeBase64String(snapshot.getDocumentId()));
            }
            docList.add(snapshot);
          }
        } catch (DBException e) {
          // See the similar log message in DBSnapshot.getDocumentHandle.
          LOG.warning("Cannot convert database record to snapshot for "
              + "record " + row + "\n" + e);
        }
      }
    }
    LOG.info(docList.size() + " document(s) to be fed to GSA at time: "
        + new Date());
    return docList;
  }
}
