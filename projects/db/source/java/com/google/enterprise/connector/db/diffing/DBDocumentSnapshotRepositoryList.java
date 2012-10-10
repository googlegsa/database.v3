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

import java.util.ArrayList;

/**
 * Building-block required by the diffing framework. Gives a collection of
 * SnapshotRepository to the diffing framework ({@link
 * DocumentSnapshotRepositoryMonitorManagerImpl class) for accessing the
 * SnapshotRepository objects. The monitor manager creates a thread for
 * each entry in the list. In this connector there is only one
 * SnapshotRepository object (as database cannot be crawled in segments)
 * hence the Monitor Manager creates only one thread to crawl the
 * database.
 */
public class DBDocumentSnapshotRepositoryList extends
    ArrayList<DBSnapshotRepository> {
  public DBDocumentSnapshotRepositoryList(RepositoryHandler repositoryHandler) {
    add(new DBSnapshotRepository(repositoryHandler));
  }
}
