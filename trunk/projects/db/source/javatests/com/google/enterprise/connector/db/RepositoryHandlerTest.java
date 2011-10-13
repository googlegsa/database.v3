package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import java.util.LinkedList;

public class RepositoryHandlerTest extends DBTestBase {

  /* @Override */
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
  }

  public void testMakeRepositoryHandlerFromConfig() {
    try {
      DBClient dbClient = getDbClient();
      RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
      assertNotNull(repositoryHandler);
    } catch (RepositoryException e) {
      fail();
    }
  }

  public void testSetCursorDB() {
    DBClient dbClient;
    try {
      int cursorDB = 5;
      dbClient = getDbClient();
      RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
      repositoryHandler.setCursorDB(cursorDB);
      assertEquals(5, repositoryHandler.getCursorDB());
    } catch (RepositoryException e) {

      fail();
    }

  }

  public void testExecuteQueryAndAddDocs() {
    try {
      DBClient dbClient = getDbClient();
      RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
      repositoryHandler.setTraversalContext(new ProductionTraversalContext());
      LinkedList<JsonDocument> jsonDocumenList = repositoryHandler.executeQueryAndAddDocs();
      assertEquals(true, jsonDocumenList.iterator().hasNext());
    } catch (RepositoryException e) {
      fail("Repository Exception in testExecuteQueryAndAddDocs");
    } catch (DBException e) {
      fail("Database Exception in testExecuteQueryAndAddDocs");
    }

  }

  /* @Override */
  protected void tearDown() throws Exception {
    super.tearDown();
    runDBScript(DROP_TEST_DB_TABLE);
  }

}
