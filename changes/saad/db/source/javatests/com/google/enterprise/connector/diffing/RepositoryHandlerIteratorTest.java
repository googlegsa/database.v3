package com.google.enterprise.connector.diffing;

import java.util.Iterator;
import java.util.LinkedList;

import com.google.enterprise.connector.db.DBConnectorConfig;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DBTestBase;


public class RepositoryHandlerIteratorTest extends DBTestBase {

	RepositoryHandlerIterator repositoryHandlerIterator;
	RepositoryHandler repositoryHandler;
	protected void setUp() throws Exception {
		super.setUp();
		runDBScript(CREATE_TEST_DB_TABLE);
		runDBScript(LOAD_TEST_DATA);
		DBConnectorConfig dbConnectorConfig=getDBConnectorConfig();
		
		repositoryHandler=RepositoryHandler.makeRepositoryHandlerFromConfig(dbConnectorConfig, null);
		repositoryHandler.setNO_OF_ROWS(2); 
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
      	Iterator<JsonDocument> recordList;
		recordList=new LinkedList<JsonDocument>().iterator();
      	RepositoryHandlerIterator.setRecordList(recordList);
      	assertEquals(true, repositoryHandlerIterator.hasNext());
		
	}
	
	//Scenario when the recordList as well as  database resulset does  not contain any more records
	public void testhasnext3()
	{
		Iterator<JsonDocument> recordList;
		try {
			recordList=repositoryHandler.executeQueryAndAddDocs().iterator();
			RepositoryHandlerIterator.setRecordList(new LinkedList<JsonDocument>().iterator());
			assertEquals(false, repositoryHandlerIterator.hasNext());

		} catch (DBException e) {
			System.out.println("DB Exception");
		}
	
	}

	

}
