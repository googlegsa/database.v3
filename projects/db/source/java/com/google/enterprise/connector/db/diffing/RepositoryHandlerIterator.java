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

import com.google.common.collect.Iterators;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom Iterator class over collection of {@link DocumentSnapshot} objects.
 */
public class RepositoryHandlerIterator implements Iterator<DocumentSnapshot> {
  private static final Logger LOG =
      Logger.getLogger(RepositoryHandlerIterator.class.getName());

  private Iterator<DocumentSnapshot> recordList;
  private final RepositoryHandler repositoryHandler;

  public void setRecordList(Iterator<DocumentSnapshot> recordList) {
    this.recordList = recordList;
  }

  /**
   * @param repositoryHandler RepositoryHandler object for fetching DB rows in
   *        DocumentSnapshot form.
   */
  public RepositoryHandlerIterator(RepositoryHandler repositoryHandler) {
    this.repositoryHandler = repositoryHandler;
    this.recordList = Iterators.emptyIterator();
  }

  /**
   * Returns true if the recordList has more elements. Else calls the
   * RepositoryHandler to ping the repositoryHandler for more records. Returns
   * true if records are found else returns false
   */
  @Override
  public boolean hasNext() throws SnapshotRepositoryRuntimeException {
    if (recordList.hasNext()) {
      return true;
    } else {
      try {
        recordList = repositoryHandler.executeQueryAndAddDocs().iterator();
        return recordList.hasNext();
      } catch (SnapshotRepositoryRuntimeException e) {
        LOG.warning("Exception in hasNext of RepositoryHandlerIterator\n"
            + e.toString());
        throw new SnapshotRepositoryRuntimeException(
            "unable to connect to repository", e);
      }
    }
  }

  /**
   * Returns the next DocumentSnapshot element in the recordList.
   */
  @Override
  public DocumentSnapshot next() {
    DocumentSnapshot snapshot = recordList.next();
    if (LOG.isLoggable(Level.FINER)) {
      LOG.finer("DBSnapshotRepository returns document with docID "
          + DocIdUtil.decodeBase64String(snapshot.getDocumentId()));
    }
    return snapshot;
  }

  /**
   * This is a read-only iterator that does not support this method.
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException(
        "Remove Operation not Supported for RepositoryHandlerIterator");
  }
}
