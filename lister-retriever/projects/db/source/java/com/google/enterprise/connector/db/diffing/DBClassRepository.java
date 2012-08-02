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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.util.diffing.SnapshotRepository;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DBClassRepository Implements the @link SnapshotRepository Interface.
 * Implemented by delegating to an {@link Iterable}<{@link JsonDocument}>
 */
public class DBClassRepository implements SnapshotRepository<DBClass> {
  private static final Logger LOG =
      Logger.getLogger(DBClassRepository.class.getName());

  private final Iterable<JsonDocument> dbFetcher;

  public DBClassRepository(Iterable<JsonDocument> DBFetcher) {
    this.dbFetcher = DBFetcher;
  }

  /* @Override */
  public Iterator<DBClass> iterator()
      throws SnapshotRepositoryRuntimeException {
    final Function<JsonDocument, DBClass> f = new ConversionFunction();
    Iterator<DBClass> it1 = Iterators.transform(dbFetcher.iterator(), f);
    return it1;
  }

  /* @Override */
  public String getName() {
    String result = DBClassRepository.class.getName();
    return result;
  }

  /**
   * Class which implements function interface for transforming JsonDocument
   * objects to DBClass Object.
   */
  private static class ConversionFunction implements
      Function<JsonDocument, DBClass> {

    /* @Override */
    public DBClass apply(JsonDocument jdoc) {
      DBClass p = DBClass.factoryFunction.apply(jdoc);
      if (LOG.isLoggable(Level.FINER)) {
        LOG.finer("DBClassRepository returns document with docID "
            + DocIdUtil.decodeBase64String(p.getDocumentId().toString()));
      }
      return p;
    }
  }
}
