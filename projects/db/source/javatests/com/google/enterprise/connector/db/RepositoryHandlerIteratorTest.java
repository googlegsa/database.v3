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

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.db.diffing.RepositoryHandlerIterator;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.Iterator;
import java.util.LinkedList;

public class RepositoryHandlerIteratorTest extends DBTestBase {

  RepositoryHandlerIterator repositoryHandlerIterator;
  RepositoryHandler repositoryHandler;

  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
    DBClient dbClient = getDbClient();

    repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
    repositoryHandler.setTraversalContext(new ProductionTraversalContext());
    repositoryHandlerIterator = new RepositoryHandlerIterator(repositoryHandler);
    repositoryHandlerIterator.setRecordList(repositoryHandler.executeQueryAndAddDocs().iterator());
  }

  public void testNext() {
    JsonDocument jsonDocument = repositoryHandlerIterator.next();
    assertNotNull(jsonDocument);
  }

  // Scenario when the recordlist contains more records.
  public void testHasNext1() {
    assertEquals(true, repositoryHandlerIterator.hasNext());
  }

  // Scenario when the recordList does not contain more records but the
  // database result set does
  public void testhasnext2() {
    Iterator<JsonDocument> recordList;
    recordList = new LinkedList<JsonDocument>().iterator();
    repositoryHandlerIterator.setRecordList(recordList);
    assertEquals(true, repositoryHandlerIterator.hasNext());
  }

  // Scenario when the recordList as well as database resulset does not
  // contain any more records
  public void testhasnext3() {
    Iterator<JsonDocument> recordList;
    try {
      // retrieve all the rows from the database
      recordList = repositoryHandler.executeQueryAndAddDocs().iterator();
      // make the recordList contain no records
      repositoryHandlerIterator.setRecordList(new LinkedList<JsonDocument>().iterator());
      assertEquals(false, repositoryHandlerIterator.hasNext());

    } catch (SnapshotRepositoryRuntimeException e) {
      fail("Database Exception in testhasnext3");
    }
  }

  /* @Override */
  protected void tearDown() throws Exception {
    super.tearDown();
    runDBScript(DROP_TEST_DB_TABLE);
  }

}
