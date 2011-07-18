package com.google.enterprise.connector.db;


import com.google.enterprise.connector.db.diffing.DBJsonDocumentFetcher;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;



public class DBJsonDocumentFetcherTest extends DBTestBase {

	/* @Override */
	protected void setUp() throws Exception {
		super.setUp();
		runDBScript(CREATE_TEST_DB_TABLE);
		runDBScript(LOAD_TEST_DATA);
	}

    public void testIterator() {


        DBClient dbClient;
        try {
            dbClient = getDbClient();
			RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null);
			repositoryHandler.setTraversalContext(new ProductionTraversalContext());
            DBJsonDocumentFetcher dbJsonDocumentFetcher=new DBJsonDocumentFetcher(repositoryHandler);
            assertTrue(dbJsonDocumentFetcher.iterator().hasNext());
        } catch (RepositoryException e) {
			fail("Repository Exception in testIterator");
        }

    }

	/* @Override */
	protected void tearDown() throws Exception {
		super.tearDown();
		runDBScript(DROP_TEST_DB_TABLE);
	}
}
