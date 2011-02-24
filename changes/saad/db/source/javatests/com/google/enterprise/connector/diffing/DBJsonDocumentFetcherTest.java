package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.db.DBConnectorConfig;
import com.google.enterprise.connector.db.DBTestBase;
import com.google.enterprise.connector.spi.RepositoryException;



public class DBJsonDocumentFetcherTest extends DBTestBase {

@Override
protected void setUp() throws Exception {
	super.setUp();
	runDBScript(CREATE_TEST_DB_TABLE);
	runDBScript(LOAD_TEST_DATA);
}
	public void testIterator() {
	
		
		DBConnectorConfig dbConnectorConfig;
		try {
			dbConnectorConfig = getDBConnectorConfig();
			RepositoryHandler repositoryHandler=RepositoryHandler.makeRepositoryHandlerFromConfig(dbConnectorConfig, null);
			DBJsonDocumentFetcher dbJsonDocumentFetcher=new DBJsonDocumentFetcher(repositoryHandler);
			assertNotNull(dbJsonDocumentFetcher.iterator().hasNext());
		} catch (RepositoryException e) {
			System.out.println("Repository Exception");		}
		
	}

}
