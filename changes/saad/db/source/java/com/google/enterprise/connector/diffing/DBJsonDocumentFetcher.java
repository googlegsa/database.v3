package com.google.enterprise.connector.diffing;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Logger;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.enterprise.connector.db.DBDocument;


public class DBJsonDocumentFetcher implements JsonDocumentFetcher{
	private static Logger LOG = Logger.getLogger(DBJsonDocumentFetcher.class.getName());
	private final LinkedList<DBDocument> DBDocumentSupplier;
	
	
	public DBJsonDocumentFetcher(
			LinkedList<DBDocument> DBDocumentSupplier) {
		    this.DBDocumentSupplier = DBDocumentSupplier;
		  }

	public Iterator<JsonDocument> iterator() {
		LinkedList<DBDocument> results = this.DBDocumentSupplier;
		 final Function<DBDocument,JsonDocument> f = new ConversionFunction();
	    return Iterators.transform(results.iterator(),f);
	  }


	private static class ConversionFunction implements Function<DBDocument,JsonDocument> {
	    /* @Override */
	    public JsonDocument apply(DBDocument dbDoc) {
	      JsonDocument p = JsonDocument.buildFromDBDocument.apply(dbDoc);
	      return p;
	    }
	  }
}
