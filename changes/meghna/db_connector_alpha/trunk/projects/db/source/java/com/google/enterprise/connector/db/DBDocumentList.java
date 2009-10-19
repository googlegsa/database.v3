// Copyright 2009 Google Inc.
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

package com.google.enterprise.connector.db;

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.DocumentList;
import com.google.enterprise.connector.spi.RepositoryException;

import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link DocumentList}.
 *
 */
public class DBDocumentList implements DocumentList {
  private final LinkedList<DBDocument> docList = new LinkedList<DBDocument>();
  private GlobalState globalState;

  /**
   * Constructs an empty document list.
   */
  public DBDocumentList(GlobalState globalState) {
    this.globalState = globalState;
  }

  public LinkedList<DBDocument> getDocList() {
    return docList;
  }

  /**
   * Saves the current state on disk. And returns a checkpoint string to the
   * Connector Manager.
   */
  @Override
  public String checkpoint() throws RepositoryException {
    try {
      globalState.saveState();
    } catch (DBException e) {
      throw new RepositoryException("Could not save checkpoint.", e);
    }
    String checkpointString;
    try {
      checkpointString  = Util.getCheckpointString(
          globalState.getQueryExecutionTime(), docList.element());
    } catch (NoSuchElementException e) {
      checkpointString  = Util.getCheckpointString(
          globalState.getQueryExecutionTime(), null);
    }
    return checkpointString;
  }

  /**
   * When this method is called by the CM, all the documents that the sent
   * are also saved in docsInFlight.
   * 
   * @return the next document or null if all have been processed.
   */
  @Override
  public Document nextDocument() {
    DBDocument doc = docList.poll();
    if (null != doc) {
      if (globalState.getDocsInFlight().size() == 0) {
        globalState.setQueryTimeForInFlightDocs(globalState.getQueryExecutionTime());
      }
      globalState.getDocsInFlight().add(doc);
    }
    return doc;
  }

  /**
   * Add a document to the list.
   *
   * @param dbDoc document to add.
   */
  public void addDocument(DBDocument dbDoc) {
    docList.add(dbDoc);
  }

  /**
   * Empties the docList.
   */
  public void clear() {
    docList.clear();
  }

  /**
   * Returns the size of the docList.
   * 
   * @return size of the docList.
   */
  public int size() {
    return docList.size();
  }

  public void addDocsInFlight(LinkedList<DBDocument> list) {
    docList.addAll(0, list);
  }
}
