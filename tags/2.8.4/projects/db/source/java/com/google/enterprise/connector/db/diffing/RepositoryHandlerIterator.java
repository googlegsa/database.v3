// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db.diffing;

import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Custom Iterator class over collection of @link JsonDocument objects.
 */
public class RepositoryHandlerIterator implements Iterator<JsonDocument> {

  private static final Logger LOG = Logger.getLogger(RepositoryHandlerIterator.class.getName());
  private Iterator<JsonDocument> recordList;
  private RepositoryHandler repositoryHandler;

  public Iterator<JsonDocument> getRecordList() {
    return recordList;
  }

  public void setRecordList(Iterator<JsonDocument> recordList) {
    this.recordList = recordList;
  }

  /**
   * @param recordList collection for holding JsonDocuments.
   * @param repositoryHandler RepositoryHandler object for fetching DB rows in
   *          JsonDocument form.
   */
  public RepositoryHandlerIterator(RepositoryHandler repositoryHandler) {
    this.repositoryHandler = repositoryHandler;
    this.recordList = new LinkedList<JsonDocument>().iterator();
  }

  /**
   * Returns true if the recordList has more elements. Else calls the
   * RepositoryHandler to ping the repositoryHandler for more records. Returns
   * true if records are found else returns false
   */
  /* @Override */
  public boolean hasNext() throws SnapshotRepositoryRuntimeException {
    if (recordList.hasNext()) {
      return true;
    } else {
      try {
        recordList = repositoryHandler.executeQueryAndAddDocs().iterator();
        if (!recordList.hasNext()) {
          return false;
        }
        return true;
      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.warning("Exception in hasnext of RepositoryHandlerIterator" + "\n"
            + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "unable to connect to repository", e);
      }
    }
  }

  /**
   * Returns the next JsonDocument element in the recordList
   */
  /* @Override */
  public JsonDocument next() {
    // TODO Auto-generated method stub

    return recordList.next();
  }

  /**
   * Implementation required for the inherited abstract method
   * Iterator<JsonDocument>.remove(). As this is a read-only iterator, the
   * remove operation does not require implementation.
   */
  /* @Override */
  public void remove() {
    // TODO Auto-generated method stub

    throw new UnsupportedOperationException(
        "Remove Operation not Supportrd for RepositoryHandlerIterator");
  }

}
