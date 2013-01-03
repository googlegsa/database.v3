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

package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.DBClassRepository;
import com.google.enterprise.connector.db.diffing.DBJsonDocumentFetcher;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

public class DBClassRepositoryTest extends DBTestBase {

  DBClassRepository dbClassRepository;

  /* @Override */
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
    DBClient dbClient = getDbClient();
    RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
    repositoryHandler.setTraversalContext(new ProductionTraversalContext());
    DBJsonDocumentFetcher dbJsonDocumentFetcher = new DBJsonDocumentFetcher(
        repositoryHandler);
    dbClassRepository = new DBClassRepository(dbJsonDocumentFetcher);
  }

  public void testIterator() {
    assertTrue(dbClassRepository.iterator().hasNext());
  }

  public void testGetName() {
    String expected = "com.google.enterprise.connector.db.diffing.DBClassRepository";
    String actual = dbClassRepository.getName();

    assertEquals(expected, actual);
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    runDBScript(DROP_TEST_DB_TABLE);
  }
}
