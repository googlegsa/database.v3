package com.google.enterprise.connector.diffing;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.logging.Logger;
import com.google.enterprise.connector.db.DBDocument;



public class DBDocumentSnapshotRepositoryList extends ArrayList<DBClassRepository> {
	private static final Logger LOG = Logger.getLogger(DBDocumentSnapshotRepositoryList.class.getName());
	
public DBDocumentSnapshotRepositoryList(RepositoryHandler repositoryHandler) {
	    JsonDocumentFetcher f = new DBJsonDocumentFetcher(repositoryHandler);
	    DBClassRepository repository =new DBClassRepository(f);
	    LOG.info("Repository Length Is:"+repository);
	    add(repository);
	  }

}
