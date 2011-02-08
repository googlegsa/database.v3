package com.google.enterprise.connector.diffing;

import java.util.Iterator;
import java.util.logging.Logger;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.enterprise.connector.db.DBDocument;


public class DBJsonDocumentFetcher implements JsonDocumentFetcher{
	private static Logger LOG = Logger.getLogger(DBJsonDocumentFetcher.class.getName());
	private final RepositoryHandler repositoryHandler;
	
	
	public DBJsonDocumentFetcher(RepositoryHandler repositoryHandler) {
		    this.repositoryHandler = repositoryHandler;
		  }

	public Iterator<JsonDocument> iterator()  {
		RepositoryHandlerIterator repositoryHandlerIterator=new RepositoryHandlerIterator(repositoryHandler);
		
		return repositoryHandlerIterator;
		/*final Function<DBDocument,JsonDocument> f = new ConversionFunction();
	    Iterator<JsonDocument> it1=Iterators.transform(repositoryHandlerIterator,f);
	    return it1;
*/	}


	/*private static class ConversionFunction implements Function<DBDocument,JsonDocument> {
	     @Override 
	    public JsonDocument apply(DBDocument dbDoc) {
	      JsonDocument p = JsonDocument.buildFromDBDocument.apply(dbDoc);
	    LOG.info("The json documents are :" +p.toJson());
	      return p;
	    }
	  }*/
}
