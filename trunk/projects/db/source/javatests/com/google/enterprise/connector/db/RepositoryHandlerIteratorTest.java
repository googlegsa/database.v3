package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.db.diffing.RepositoryHandlerIterator;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

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

    JsonDocument jsonDocument = (JsonDocument) repositoryHandlerIterator.next();
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

    } catch (DBException e) {
      fail("Database Exception in testhasnext3");
    }

  }

  /* @Override */
  protected void tearDown() throws Exception {
    super.tearDown();
    runDBScript(DROP_TEST_DB_TABLE);
  }

}
