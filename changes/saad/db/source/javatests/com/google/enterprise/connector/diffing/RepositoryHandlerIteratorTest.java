package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.db.DBConnectorConfig;
import com.google.enterprise.connector.db.DBTestBase;


public class RepositoryHandlerIteratorTest extends DBTestBase {

	RepositoryHandlerIterator repositoryHandlerIterator;
	
	protected void setUp() throws Exception {
		DBConnectorConfig dbConnectorConfig=getDBConnectorConfig();
		RepositoryHandler repositoryHandler=RepositoryHandler.makeRepositoryHandlerFromConfig(dbConnectorConfig, null);
		repositoryHandler.setBatchHint(2);
		repositoryHandlerIterator=new RepositoryHandlerIterator(repositoryHandler);
	}
	
	
	public void testNext() {
		
	JsonDocument jsonDocument=(JsonDocument)repositoryHandlerIterator.next();
	assertNotNull(jsonDocument);
	}
	
	//Scenario when the recordlist contains more records.	
	public void testHasNext1() {
	assertEquals(true, repositoryHandlerIterator.hasNext());
	}
	
	
	//Scenario when the recordList does not contain more records but the database result set does
	public void testhasnext2()
	{
		
		
	}
	
	//Scenario when the database resulset does  not contain any more records
	public void testhasnext3()
	{
		
	}

	

}
