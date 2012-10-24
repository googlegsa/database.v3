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
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepository;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.Iterator;

/**
 * DBSnapshotRepository Implements the @link SnapshotRepository Interface.
 * Implemented by delegating to an {@link Iterable}<{@link DocumentSnapshot}>
 */
/*
 * TODO: This implementation isn't really iterable, because we only
 * get one instance of Iterator.
 */
public class DBSnapshotRepository
    implements SnapshotRepository<DocumentSnapshot> {
  private final Iterator<DocumentSnapshot> iterator;

  public DBSnapshotRepository(RepositoryHandler repositoryHandler) {
    this.iterator = new RepositoryHandlerIterator(repositoryHandler);
  }

  /* @Override */
  public Iterator<DocumentSnapshot> iterator()
      throws SnapshotRepositoryRuntimeException {
    return iterator;
  }

  /* @Override */
  public String getName() {
    return DBSnapshotRepository.class.getName();
  }
}
