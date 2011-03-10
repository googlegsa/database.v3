package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.DBClassRepository;
import com.google.enterprise.connector.db.diffing.DBClient;
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
		RepositoryHandler repositoryHandler = RepositoryHandler.makeRepositoryHandlerFromConfig(dbClient, null, "2", "");
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
