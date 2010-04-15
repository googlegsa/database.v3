// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.joda.time.DateTime;

import com.google.enterprise.connector.spi.DocumentList;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.TraversalManager;

/**
 * {@link TraversalManager} implementation for the DB connector.
 */
public class DBTraversalManager implements TraversalManager {
	private static final Logger LOG = Logger.getLogger(DBTraversalManager.class.getName());
	private DBClient dbClient;
	private GlobalState globalState;
	private String xslt;

	// Limit on the batch size.
	private int batchHint = 100;

	/**
	 * Creates a DBTraversalManager.
	 * 
	 * @param dbClient DBClient to talk to the database.
	 */
	public DBTraversalManager(DBClient dbClient, String xslt) {
		this.dbClient = dbClient;
		this.xslt = xslt;
		this.globalState = new GlobalState(dbClient.getGoogleConnectorWorkDir());
		globalState.loadState();
	}

	/**
	 * getter method for dbClient is added so that other part of application can
	 * make use of it whenever required to perform database operation
	 * 
	 * @return DBClient to perform database operations
	 */
	public DBClient getDbClient() {
		return dbClient;
	}

	/**
	 * Resumes traversal from the point where it was left in the previous run.
	 * If the checkpoint string passed to it is the same as the connector thinks
	 * should be then the connector assumes the docsInFlight actually made it.
	 * If it is the checkpoint string of the previous traversal, then the
	 * connector assumes that the docsInFlight were not able to make it. So it
	 * send them again. Of course, the caveat is that the docsInflight actually
	 * made but there was a problem in persisting the checkpoint string. In this
	 * case, the docs will just be sent again.
	 */
	/* @Override */
	public DocumentList resumeTraversal(String checkpointStr)
			throws RepositoryException {
		LOG.info("Traversal resumed at " + new Date() + " from checkpoint "
				+ checkpointStr);
		try {
			String oldCheckpointStr;
			try {
				oldCheckpointStr = Util.getCheckpointString(globalState.getQueryTimeForInFlightDocs(), globalState.getDocsInFlight().element());
			} catch (NoSuchElementException e) {
				oldCheckpointStr = Util.getCheckpointString(null, null);
			}
			String currentCheckpointStr;
			try {
				currentCheckpointStr = Util.getCheckpointString(globalState.getQueryExecutionTime(), globalState.getDocQueue().getDocList().element());
			} catch (NoSuchElementException e) {
				currentCheckpointStr = Util.getCheckpointString(globalState.getQueryExecutionTime(), null);
			}

			/*
			 * The currentCheckpointStr can contain NO_DOCID if the docQueue
			 * does not have anymore docs. In this case if the checkpointStr has
			 * NO_DOCID, it should match the currentCheckpointStr. But in case
			 * where the next traversal is called and the checkpoint is not
			 * persisted and the previous checkpoint with NO_DOCID is called,
			 * then the system should behave as if it got the oldCheckpointStr.
			 * E.g., consider a case with query time t1 and 0 docs in docQueue,
			 * the currentCheckpointStr will be (t1)NO_DOCID. If this the
			 * checkpointStr in the next resumeTraversal, then docsInFlight
			 * needs to be cleared. After this a new traversal starts at time t2
			 * with oldCheckpointStr as (t2)X and currentCheckpointStr as (t2)Y
			 * with X and Y as the docIds of the first documents in docsInFlight
			 * and docQueue respectively. If everything goes fine, the
			 * checkpointStr received for the next resumeTraversal should be
			 * (t2)Y. But if that doesn't happen, then CM will send previous
			 * checkpoint which in this case is (t1)NO_DOCID. But this one is
			 * equivalent to (t2)X.
			 */
			if (checkpointStr.equals(currentCheckpointStr)) {
				globalState.getDocsInFlight().clear();
			} else if (checkpointStr.equals(oldCheckpointStr)
					|| checkpointStr.contains(Util.NO_DOCID)) {
				globalState.getDocQueue().addDocsInFlight(globalState.getDocsInFlight());
				globalState.getDocsInFlight().clear();
			}
			return traverseDB();
		} catch (DBException e) {
			throw new RepositoryException("Could not resume traversal");
		}
	}

	/* @Override */
	public void setBatchHint(int batchHint) {
		assert batchHint > 0;
		this.batchHint = batchHint;
	}

	/**
	 * @return the current batch-size hint.
	 */
	public int getBatchHint() {
		return batchHint;
	}

	/**
	 * @return the globalState.
	 */
	GlobalState getGlobalState() {
		return globalState;
	}

	/**
	 * Starts the traversal of the database. The docList contains one DBDocument
	 * per row.
	 * 
	 * @return docList DBDocumentList corresponding to the rows in the DB.
	 */
	/* @Override */
	public DocumentList startTraversal() throws RepositoryException {
		LOG.info("Traversal of database " + dbClient.getDBContext().getDbName()
				+ " is started at : " + new Date());
		try {
			// Making sure the old state is gone.
			globalState.clearState();
		} catch (DBException e1) {
			throw new RepositoryException("Could not clear old state", e1);
		}
		try {
			return traverseDB();
		} catch (DBException e) {
			throw new RepositoryException("Could not start traversal", e);
		}
	}

	/**
	 * Traverses the DB. It first gets 3 times the batch hint rows. It converts
	 * these rows into DBDcouments and inserts them in the global doc queue. It
	 * also remembers which row in the DB to fetch next. When one complete sweep
	 * of the DB is done. It adds the documents not found in the latest sweep to
	 * the doc queue and marks them for deletion before adding docs from the
	 * next sweep.
	 * 
	 * @return DBDcoumentList document list to be consumed by the CM.
	 * @throws DBException
	 */
	private DBDocumentList traverseDB() throws DBException {

		List<Map<String, Object>> rows;
		if (0 == globalState.getDocQueue().size()) {
			// Multiplying batch hint by 3 (picked randomly) to prefetch some
			// results.
			rows = executeQueryAndAddDocs();
			// You've reached the end of the DB or the DB is empty.
			if (0 == rows.size()) {
				int recordCOunt = globalState.getCursorDB();
				globalState.markNewDBTraversal();
				/*
				 * globalState.getDocQueue().size() can be non-zero if there are
				 * any documents to delete.
				 */
				/*
				 * Connector returns null value to notify connector that
				 * traversing has reached the end of the DB and it to wait till
				 * retry delay time lapse before starting next crawl cycle. Save
				 * the current state into xml file at the end of each traversal
				 * cycle.
				 */
				if (0 == globalState.getDocQueue().size()) {
					globalState.saveState();
					LOG.info("Crawl cycle of database "
							+ dbClient.getDBContext().getDbName()
							+ " is completed at: " + new Date() + "\nTotal "
							+ recordCOunt
							+ " records are crawled during this crawl cycle");
					return null;
				}
			}
		}
		return globalState.getDocQueue();
	}

	private List<Map<String, Object>> executeQueryAndAddDocs()
			throws DBException {
		List<Map<String, Object>> rows = dbClient.executePartialQuery(globalState.getCursorDB(), 3 * batchHint);
		globalState.setCursorDB(globalState.getCursorDB() + rows.size());
		globalState.setQueryExecutionTime(new DateTime());
		for (Map<String, Object> row : rows) {
			globalState.addDocument(Util.rowToDoc(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), xslt));
		}
		LOG.info(globalState.getDocQueue().size()
				+ " document(s) to be fed to GSA");
		return rows;
	}
}
