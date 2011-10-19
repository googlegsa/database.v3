package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

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

  public void testExecuteQueryAndAddDocsForParameterizedQuery() {
    // Testing the connector for parameterized crawl query
    String sqlQuery = "SELECT * FROM TestEmpTable where id between #minvalue# and #maxvalue#";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    dbContext.setParameterizedQueryFlag(true);
    dbContext.setMinValue(1);
    dbContext.setMaxValue(2);
    DBClient dbClient;
    try {
      dbClient = new DBClient(dbContext);
      RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
      repositoryHandler.setTraversalContext(new ProductionTraversalContext());
      LinkedList<JsonDocument> jsonDocumenList = repositoryHandler.executeQueryAndAddDocs();
      JsonDocument jsonDocument = (JsonDocument) jsonDocumenList.iterator().next();
      assertEquals("MQ", jsonDocument.getDocumentId());
    } catch (DBException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
