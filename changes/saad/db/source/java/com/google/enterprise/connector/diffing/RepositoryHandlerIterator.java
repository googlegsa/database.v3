package com.google.enterprise.connector.diffing;

import java.util.Iterator;
import java.util.logging.Logger;
import com.google.enterprise.connector.db.DBException;
;

/*
 * Custom Iterator class over collection of @link JsonDocument objects. 
 * 
 * */
public class RepositoryHandlerIterator implements Iterator<JsonDocument> {

	private static final Logger LOG = Logger.getLogger(RepositoryHandlerIterator.class.getName()); 
	private static Iterator<JsonDocument> recordList;
	private static RepositoryHandler repositoryHandler;

	/*
	 * @param recordList collection for holding JsonDocuments.
	 * @param repositoryHandler RepositoryHandler object for fetching DB rows in JsonDocument form.
	 *  
	 * */
	public RepositoryHandlerIterator(RepositoryHandler repositoryHandler)
	{
		try {
			RepositoryHandlerIterator.repositoryHandler=repositoryHandler;
			recordList=repositoryHandler.executeQueryAndAddDocs().iterator();
		} catch (DBException e) {
			// TODO Auto-generated catch block
		LOG.info("DBException in RepositoryHandlerIterator");
		}
	}
	
	@Override
	/*
	 * Returns true if the recordList has more elements.
	 * Else calls the RepositoryHandler to ping the repositoryHandler for more records.
	 * Returns true if records are found else returns false 
	 * 
	 * */
	public boolean hasNext() {
		// TODO Auto-generated method stub
		if(recordList.hasNext())
		{
			return true;
		}
		else
		{
			try {
				recordList=repositoryHandler.executeQueryAndAddDocs().iterator();
				if(!recordList.hasNext())
				{
					return false;
				}
				return true;
			} catch (DBException e) {
				// TODO Auto-generated catch block
				LOG.info("DBEXception in hasnext of RepositoryHandlerIterator");
			}
			
			
		}
		return false;
	}

	@Override
	//Returns the next JsonDocument element in the recordList
	  
	
	public JsonDocument next() {
		// TODO Auto-generated method stub
		
		return (JsonDocument)recordList.next();
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

}
