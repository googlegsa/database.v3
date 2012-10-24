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

package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import java.util.List;

public class RepositoryHandlerTest extends DBTestBase {

  /* @Override */
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
  }

  public void testMakeRepositoryHandlerFromConfig() {
    RepositoryHandler repositoryHandler =
        RepositoryHandler.makeRepositoryHandlerFromConfig(getDbContext(), null);
    assertNotNull(repositoryHandler);
  }

  public void testSetCursorDB() {
    int cursorDB = 5;
    RepositoryHandler repositoryHandler =
        RepositoryHandler.makeRepositoryHandlerFromConfig(getDbContext(), null);
    repositoryHandler.setCursorDB(cursorDB);
    assertEquals(5, repositoryHandler.getCursorDB());
  }

  public void testExecuteQueryAndAddDocsForParameterizedQuery() {
    // Testing the connector for parameterized crawl query
    String sqlQuery = "SELECT * FROM TestEmpTable where id > #value#";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    dbContext.setParameterizedQueryFlag(true);
    RepositoryHandler repositoryHandler =
        RepositoryHandler.makeRepositoryHandlerFromConfig(dbContext, null);
    repositoryHandler.setTraversalContext(new ProductionTraversalContext());
    List<DocumentSnapshot> snapshotList =
        repositoryHandler.executeQueryAndAddDocs();
    DocumentSnapshot snapshot = snapshotList.iterator().next();
    assertEquals("MQ", snapshot.getDocumentId());
  }

  public void testExecuteQueryAndAddDocs() {
    try {
      RepositoryHandler repositoryHandler =
          RepositoryHandler.makeRepositoryHandlerFromConfig(getDbContext(), null);
      repositoryHandler.setTraversalContext(new ProductionTraversalContext());
      List<DocumentSnapshot> jsonDocumenList =
          repositoryHandler.executeQueryAndAddDocs();
      assertEquals(true, jsonDocumenList.iterator().hasNext());
    } catch (SnapshotRepositoryRuntimeException e) {
      fail("Database Exception in testExecuteQueryAndAddDocs");
    }
  }

  /* @Override */
  protected void tearDown() throws Exception {
    super.tearDown();
    runDBScript(DROP_TEST_DB_TABLE);
  }

}
