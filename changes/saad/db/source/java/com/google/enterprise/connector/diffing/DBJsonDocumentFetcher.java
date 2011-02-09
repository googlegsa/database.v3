package com.google.enterprise.connector.diffing;

import java.util.Iterator;
import java.util.logging.Logger;



public class DBJsonDocumentFetcher implements JsonDocumentFetcher{
	private static Logger LOG = Logger.getLogger(DBJsonDocumentFetcher.class.getName());
	private final RepositoryHandler repositoryHandler;
	
	
	public DBJsonDocumentFetcher(RepositoryHandler repositoryHandler) {
		    this.repositoryHandler = repositoryHandler;
		  }

	public Iterator<JsonDocument> iterator()  {
		RepositoryHandlerIterator repositoryHandlerIterator=new RepositoryHandlerIterator(repositoryHandler);
		
		return repositoryHandlerIterator;
	}


	
}
