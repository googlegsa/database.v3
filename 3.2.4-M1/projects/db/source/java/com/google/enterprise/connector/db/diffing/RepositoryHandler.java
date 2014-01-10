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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;
import com.google.enterprise.connector.util.diffing.TraversalContextManager;

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

  private final DBContext dbContext;
  private final DBClient dbClient;
  private final TraversalContextManager traversalContextManager;
  private final QueryStrategy queryStrategy;
  private final DocumentBuilder docBuilder;

  private TraversalContext traversalContext;

  public static RepositoryHandler makeRepositoryHandlerFromConfig(
      DBContext dbContext, TraversalContextManager traversalContextManager) {
    return new RepositoryHandler(dbContext, traversalContextManager);
  }

  private RepositoryHandler(
      DBContext dbContext, TraversalContextManager traversalContextManager) {
    this.dbContext = dbContext;
    this.dbClient = dbContext.getClient();
    this.traversalContextManager = traversalContextManager;

    queryStrategy = (dbContext.isParameterizedQueryFlag())
        ? new ParameterizedQueryStrategy() : new PartialQueryStrategy();
    docBuilder = DocumentBuilder.getInstance(dbContext, traversalContext);
  }

  private interface QueryStrategy {
    List<Map<String, Object>> executeQuery();
    void resetCursor();
    void updateCursor(List<Map<String, Object>> rows);
    void logComplete();
  }

  private class PartialQueryStrategy implements QueryStrategy {
    private int skipRows = 0;

    @Override
    public List<Map<String, Object>> executeQuery() {
      return dbClient.executePartialQuery(skipRows,
            dbContext.getNumberOfRows());
    }

    @Override
    public void resetCursor() {
      skipRows = 0;
    }

    @Override
    public void updateCursor(List<Map<String, Object>> rows) {
      skipRows += rows.size();
    }

    @Override
    public void logComplete() {
      LOG.info("Total " + skipRows
          + " records are crawled during this crawl cycle");
    }
  }

  private class ParameterizedQueryStrategy implements QueryStrategy {
    private Integer keyValue;
    private String primaryKeyColumn = null;

    public ParameterizedQueryStrategy() {
      keyValue = dbContext.getMinValue();
    }

    @Override
    public List<Map<String, Object>> executeQuery() {
      return dbClient.executeParameterizePartialQuery(keyValue);
    }

    @Override
    public void resetCursor() {
      keyValue = dbContext.getMinValue();
    }

    /**
     * If user enters primary key column name in different case in database
     * connector configuration form, we need to map primary key column name
     * entered by user with actual column name in query. Below block of code map
     * the primary key column name entered by user with actual column name in
     * result set(map).
     */
    private String getPrimaryKeyColumn(Set<String> columnNames) {
      try {
        return dbContext.getPrimaryKeyColumns(columnNames).get(0);
      } catch (DBException e) {
        throw new SnapshotRepositoryRuntimeException(
            "Error getting the primary key column.", e);
      }
    }

    /**
     *Updates the keyValue with the highest order key.
     */
    @Override
    public void updateCursor(List<Map<String, Object>> rows) {
      Preconditions.checkArgument(rows.size() > 0);
      if (primaryKeyColumn == null) {
        primaryKeyColumn = getPrimaryKeyColumn(rows.get(0).keySet());
      }
      for (Map<String, Object> row : rows) {
        String newKeyValueString = row.get(primaryKeyColumn).toString();
        Integer newKeyValue = Integer.parseInt(newKeyValueString);
        if (keyValue < newKeyValue) {
          keyValue = newKeyValue;
        }
      }
    }

    @Override
    public void logComplete() {
      LOG.info("No records returned for keyValue= " + keyValue);
    }
  }

  /**
   * Function for fetching database rows and providing a collection of
   * snapshots.
   */
  public List<DocumentSnapshot> executeQueryAndAddDocs()
      throws SnapshotRepositoryRuntimeException {
    List<Map<String, Object>> rows = null;

    try {
      rows = queryStrategy.executeQuery();
    } catch (SnapshotRepositoryRuntimeException e) {
      LOG.info("Repository Unreachable. Resetting DB cursor to "
          + "start traversal from begining after recovery.");
      queryStrategy.resetCursor();
      LOG.warning("Unable to connect to the database\n" + e.toString());
      throw new SnapshotRepositoryRuntimeException(
          "Unable to connect to the database.", e);
    }
    if (rows.size() == 0) {
      queryStrategy.logComplete();
      LOG.info("Crawl cycle of database is complete. Resetting DB cursor to "
          + "start traversal from begining");
      queryStrategy.resetCursor();
    } else {
      queryStrategy.updateCursor(rows);
    }

    if (traversalContext == null) {
      LOG.info("Setting Traversal Context");
      traversalContext = traversalContextManager.getTraversalContext();
      JsonDocument.setTraversalContext(traversalContext);
    }

    return getDocList(rows);
  }

  private List<DocumentSnapshot> getDocList(List<Map<String, Object>> rows) {
    LOG.log(Level.FINE, "Building document snapshots for {0} rows.",
        rows.size());
    List<DocumentSnapshot> docList = Lists.newArrayList();
    for (Map<String, Object> row : rows) {
      try {
        DocumentSnapshot snapshot = docBuilder.getDocumentSnapshot(row);
        if (snapshot != null) {
          if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("DBSnapshotRepository returns document with docID "
                + snapshot.getDocumentId());
          }
          docList.add(snapshot);
        }
      } catch (DBException e) {
        // See the similar log message in DBSnapshot.getDocumentHandle.
        LOG.log(Level.WARNING, "Cannot convert database record to snapshot "
            + "for record " + row, e);
      }
    }
    LOG.info(docList.size() + " document(s) to be fed to GSA");
    return docList;
  }
}
