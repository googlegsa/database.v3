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
import java.util.logging.Logger;

/**
 * Building-block required by the diffing framework. Gives a collection of
 * SnapshotRepository to the diffing framework(@link
 * DocumentSnapshotRepositoryMonitorManagerImpl class) for accessing the
 * SnapshotRepository's.Depending upon the number of SnapshotRepository's the
 * Monitor Manager creates number of monitor threads.But in case of Database
 * connector the List consists of only one SnapshotRepository Object(as database
 * cannot be crawled in segments) hence the Monitor Manager creates only one
 * thread to crawl the database.
 */
public class DBDocumentSnapshotRepositoryList extends
    ArrayList<DBClassRepository> {
  private static final Logger LOG =
      Logger.getLogger(DBDocumentSnapshotRepositoryList.class.getName());

  public DBDocumentSnapshotRepositoryList(RepositoryHandler repositoryHandler) {
    JsonDocumentFetcher f = new DBJsonDocumentFetcher(repositoryHandler);
    DBClassRepository repository = new DBClassRepository(f);
    LOG.info("Repository Length Is:" + repository);
    add(repository);
  }
}
