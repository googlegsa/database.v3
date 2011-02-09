package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.db.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.diffing.TraversalContextManager;




public class RepositoryHandler{
	private static final Logger LOG = Logger.getLogger(RepositoryHandler.class.getName());
	private DBClient dbClient;
	private String xslt;
	private static TraversalContextManager traversalContextManager;
	private int cursorDB = 0;

   // Limit on the batch size.
	private int batchHint = 100;

	// EXC_NORMAL represents that DB Connector is running in normal mode
	private static final int MODE_NORMAL = 1;

	// EXC_METADATA_URL represents that DB Connector is running for indexing
	// External Metadada
	private static final int MODE_METADATA_URL = 2;

	// EXC_BLOB represents that DB Connector is running for indexing BLOB
	// data
	private static final int MODE_METADATA_BASE_URL = 3;

	// EXC_CLOB represents that DB Connector is running for indexing CLOB
	// data
	private static final int MODE_BLOB_CLOB = 4;

	// current execution mode
	private int currentExcMode = -1;

	public RepositoryHandler() {

	}
	
	
	public static RepositoryHandler makeRepositoryHandlerFromConfig(DBConnectorConfig dbConnectorConfig,TraversalContextManager traversalContextManager) {

		RepositoryHandler repositoryHandler=new RepositoryHandler();
		RepositoryHandler.traversalContextManager=traversalContextManager;
		repositoryHandler.cursorDB=0;
		repositoryHandler.dbClient = dbConnectorConfig.getDbClient();
		repositoryHandler.xslt = dbConnectorConfig.getXslt();
		return repositoryHandler;
	}



	

	public  int getCursorDB() {
		return cursorDB;
	}

	public  void setCursorDB(int cursorDB) {
		this.cursorDB = cursorDB;
	}

	public LinkedList<JsonDocument> executeQueryAndAddDocs()
	throws DBException {
		LinkedList<JsonDocument> docList = new LinkedList<JsonDocument>();
		List<Map<String, Object>> rows = dbClient.executePartialQuery(cursorDB, 2);
		if(rows.size()==0)
		{
			setCursorDB(0);
		}
		else
		{
			setCursorDB(getCursorDB() + rows.size());
		}
		JsonDocument jsonDoc = null;
		if (rows != null && rows.size() > 0) {

			currentExcMode = getExecutionScenario(dbClient.getDBContext());
			String logMessage = getExcLogMessage(currentExcMode);
			LOG.info(logMessage);

			switch (currentExcMode) {

			// execute the connector for metadata-url feed
			case MODE_METADATA_URL:

				for (Map<String, Object> row : rows) {
					jsonDoc = Util.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), "",traversalContextManager.getTraversalContext());
					docList.add(jsonDoc);
				}
				break;

				// execute the connector for BLOB data
			case MODE_METADATA_BASE_URL:
				jsonDoc = null;
				for (Map<String, Object> row : rows) {
					jsonDoc = Util.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), Util.WITH_BASE_URL,traversalContextManager.getTraversalContext()
							);
					docList.add(jsonDoc);
				}

				break;

				// execute the connector for CLOB data 
			case MODE_BLOB_CLOB:
				jsonDoc = null;
				for (Map<String, Object> row : rows) {
					jsonDoc = Util.largeObjectToDoc(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), traversalContextManager.getTraversalContext());
					docList.add(jsonDoc);
				}

				break;

				// execute the connector in normal mode
			default:
				for (Map<String, Object> row : rows) {
					jsonDoc=Util.rowToDoc(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), xslt, dbClient.getDBContext(),traversalContextManager.getTraversalContext());
					docList.add(jsonDoc);
				}
				break;
			}
		}

		return docList;
	}

	private int getExecutionScenario(DBContext dbContext) {

		String extMetaType = dbContext.getExtMetadataType();
		String lobField = dbContext.getLobField();
		String docURLField = dbContext.getDocumentURLField();
		String docIdField = dbContext.getDocumentIdField();
		if (extMetaType != null && extMetaType.trim().length() > 0
				&& !extMetaType.equals(DBConnectorType.NO_EXT_METADATA)) {
			if (extMetaType.equalsIgnoreCase(DBConnectorType.COMPLETE_URL)
					&& (docURLField != null && docURLField.trim().length() > 0)) {
				return MODE_METADATA_URL;
			} else if (extMetaType.equalsIgnoreCase(DBConnectorType.DOC_ID)
					&& (docIdField != null && docIdField.trim().length() > 0)) {
				return MODE_METADATA_BASE_URL;
			} else if (extMetaType.equalsIgnoreCase(DBConnectorType.BLOB_CLOB)
					&& (lobField != null && lobField.trim().length() > 0)) {
				return MODE_BLOB_CLOB;
			} else {
				/*
				 * Explicitly change the mode of execution as user may switch
				 * from "External Metadata Feed" mode to
				 * "Content Feed(for text data)" mode.
				 */
				dbContext.setExtMetadataType(DBConnectorType.NO_EXT_METADATA);
				return MODE_NORMAL;
			}
		} else {
			/*
			 * Explicitly change the mode of execution as user may switch from
			 * "External Metadata Feed" mode to "Content Feed(for text data)"
			 * mode.
			 */
			dbContext.setExtMetadataType(DBConnectorType.NO_EXT_METADATA);
			return MODE_NORMAL;
		}
	}

	private static String getExcLogMessage(int excMode) {

		switch (excMode) {

		case MODE_METADATA_URL: {
			/*
			 * execution mode: Externam Metadata feed using complete document
			 * URL
			 */
			return " DB Connector is running in External Metadata feed mode with complete document URL";
		}
		case MODE_METADATA_BASE_URL: {
			/*
			 * execution mode: Externam Metadata feed using Base URL and
			 * document Id
			 */
			return " DB Connector is running in External Metadata feed mode with Base URL and document ID";
		}
		case MODE_BLOB_CLOB: {
			/*
			 * execution mode: Content feed mode for BLOB/CLOB data.
			 */
			return " DB Connector is running in Content Feed Mode for BLOB/CLOB data";
		}

		default: {
			/*
			 * execution mode: Content feed mode for Text data.
			 */return " DB Connector is running in content feed mode for text data";
		}
		}

	}


}
