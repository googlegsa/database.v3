package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.db.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.spi.RepositoryException;



public class RepositoryHandler {
	private static final Logger LOG = Logger.getLogger(DBTraversalManager.class.getName());
	private DBClient dbClient;
	private String xslt;
	private TraversalContext traversalContext=null;
	private int cursorDB = 0;
	private LinkedList<DBDocument> docList = new LinkedList<DBDocument>();
	
	
	public  int getCursorDB() {
		return cursorDB;
	}

	public  void setCursorDB(int cursorDB) {
		this.cursorDB = cursorDB;
	}

	
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
	public static LinkedList<DBDocument> makeRepositoryHandlerFromConfig(DBConnectorConfig dbConnectorConfig) {
	     
		RepositoryHandler repositoryHandler=new RepositoryHandler();
		repositoryHandler.cursorDB=0;
		repositoryHandler.dbClient = dbConnectorConfig.getDbClient();
		repositoryHandler.xslt = dbConnectorConfig.getXslt();
		repositoryHandler.startTraversal();
		return repositoryHandler.docList;
	}
	

	
	public void setTraversalContext(TraversalContext traversalContext) {
		this.traversalContext = traversalContext;
	}

	public void addDocument(DBDocument dbDoc) {
			docList.add(dbDoc);
		}

	  
    public void startTraversal()
	     {
			  
		    try {
					traverseDB();
				} catch (DBException e) {
					
					LOG.info("DBException");
				} catch (RepositoryException e) {
					
					LOG.info("RepositoryException");
				}
	     }
	  
	  
	  private void traverseDB() throws DBException, RepositoryException {
		  
		  List<Map<String, Object>> rows=executeQueryAndAddDocs();
		  LOG.info("The rows crawled are"+rows);
	 }
	  
	  private List<Map<String, Object>> executeQueryAndAddDocs()
		throws DBException {
			List<Map<String, Object>> rows = dbClient.executePartialQuery(cursorDB, 3 * batchHint);

			setCursorDB(getCursorDB() + rows.size());
			DBDocument dbDoc = null;
			if (rows != null && rows.size() > 0) {

				currentExcMode = getExecutionScenario(dbClient.getDBContext());
				String logMessage = getExcLogMessage(currentExcMode);
				LOG.info(logMessage);

				switch (currentExcMode) {

				// execute the connector for metadata-url feed
				case MODE_METADATA_URL:

					for (Map<String, Object> row : rows) {
						dbDoc = Util.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), "",this.traversalContext);
						this.docList.add(dbDoc);
					}
					break;

					// execute the connector for BLOB data
				case MODE_METADATA_BASE_URL:
					dbDoc = null;
					for (Map<String, Object> row : rows) {
						dbDoc = Util.generateMetadataURLFeed(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), Util.WITH_BASE_URL,this.traversalContext);
						this.docList.add(dbDoc);
					}

					break;

					// execute the connector for CLOB data 
				case MODE_BLOB_CLOB:
					dbDoc = null;
					for (Map<String, Object> row : rows) {
						dbDoc = Util.largeObjectToDoc(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), dbClient.getDBContext(), traversalContext);
						this.docList.add(dbDoc);
					}

					break;

					// execute the connector in normal mode
				default:
					for (Map<String, Object> row : rows) {
						dbDoc=Util.rowToDoc(dbClient.getDBContext().getDbName(), dbClient.getPrimaryKeys(), row, dbClient.getDBContext().getHostname(), xslt, dbClient.getDBContext(),this.traversalContext);
						this.docList.add(dbDoc);
					}
					break;
				}
			}

			return rows;
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
