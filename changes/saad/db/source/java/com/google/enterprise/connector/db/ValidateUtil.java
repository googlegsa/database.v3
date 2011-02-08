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


import com.google.enterprise.connector.spi.ConnectorType;
import com.ibatis.common.jdbc.SimpleDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Implementation of Validation Classes for {@link ConnectorType} .
 */
public class ValidateUtil  {
	private static final Logger LOG = Logger.getLogger(DBConnectorType.class.getName());
	private static final String TEST_CONNECTIVITY = "TEST_CONNECTIVITY";
	private static final String TEST_DRIVER_CLASS = "TEST_DRIVER_CLASS";
	private static final String TEST_SQL_QUERY = "TEST_SQL_QUERY";
	private static final String TEST_PRIMARY_KEYS = "TEST_PRIMARY_KEYS";
	private static final String INVALID_COLUMN_NAME = "INVALID_COLUMN_NAME";
	private static final String FQDN_HOSTNAME = "FQDN_HOSTNAME";
	private static final String MISSING_ATTRIBUTES = "MISSING_ATTRIBUTES";
	private static final String REQ_FIELDS = "REQ_FIELDS";
	// Red asterisk for required fields.
	public static final String RED_ASTERISK = "<font color=\"RED\">*</font>";

	
	private static final String PASSWORD = "password";
	public static final String BOLD_TEXT_START = "<b>";
	public static final String BOLD_TEXT_END = "</b>";
    public static final String COMPLETE_URL = "url";
	public static final String DOC_ID = "docId";
	public static final String BLOB_CLOB = "lob";
    private static final String HOSTNAME = "hostname";
	private static final String CONNECTION_URL = "connectionUrl";
	private static final String LOGIN = "login";
	private static final String DRIVER_CLASS_NAME = "driverClassName";
	private static final String DB_NAME = "dbName";
	private static final String SQL_QUERY = "sqlQuery";
	private static final String PRIMARY_KEYS_STRING = "primaryKeysString";
	private static final String XSLT = "xslt";
	private static final String LAST_MODIFIED_DATE_FIELD = "lastModifiedDate";
	private static final String DOC_TITLE_FIELD = "documentTitle";
	// AuthZ Query
	private static final String AUTHZ_QUERY = "authZQuery";
	private static final String INVALID_AUTH_QUERY = "INVALID_AUTH_QUERY";
	private static final String DOCUMENT_URL_FIELD = "documentURLField";
	private static final String DOCUMENT_ID_FIELD = "documentIdField";
	private static final String BASE_URL = "baseURL";
	private static final String CLOB_BLOB_FIELD = "lobField";
	private static final String FETCH_URL_FIELD = "fetchURLField";
		public static final String NO_EXT_METADATA = "noExt";


	private final Set<String> configKeys;
	/*
	 * List of required fields.
	 */
	List<String> requiredFields = Arrays.asList(new String[] { HOSTNAME,
			CONNECTION_URL, DB_NAME, LOGIN, DRIVER_CLASS_NAME, SQL_QUERY,
			PRIMARY_KEYS_STRING });

	/**
	 * @param configKeys names of required configuration variables.
	 */
	public ValidateUtil(Set<String> configKeys) {
		if (configKeys == null) {
			throw new RuntimeException("configKeys must be non-null");
		}
		this.configKeys = configKeys;
	}

	
	


	/**
	 * Tests the connectivity to the database.
	 */
	private static class TestDbFields implements ConfigValidation {
		private static final String JDBC_DRIVER_STR = "JDBC.Driver";
		private static final String JDBC_CONNECTION_URL_STR = "JDBC.ConnectionURL";
		private static final String JDBC_USERNAME_STR = "JDBC.Username";
		private static final String JDBC_PASSWORD_STR = "JDBC.Password";

		private String driverClassName = null;
		private String login = null;
		private String connectionUrl = null;
		private String password = null;
		private Map<String, String> config;
		private String message = "";
		private boolean success = false;
		private List<String> problemFields = new ArrayList<String>();
		private ResourceBundle res;
		List<String> columnNames = new ArrayList<String>();

		private static final String USERNAME_PLACEHOLDER = "#username#";
		private static final String DOCI_IDS_PLACEHOLDER = "$docIds$";

		Statement stmt = null;
		Connection conn = null;
		ResultSet resultSet = null;
		boolean result = false;
		SimpleDataSource sds = null;

		public TestDbFields(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}

		private boolean testDriverClass() {
			if (driverClassName != null && connectionUrl != null
					&& login != null && password != null) {
				Map<String, String> jdbcProps = new TreeMap<String, String>();
				jdbcProps.put(JDBC_CONNECTION_URL_STR, connectionUrl);
				jdbcProps.put(JDBC_DRIVER_STR, driverClassName);
				jdbcProps.put(JDBC_USERNAME_STR, login);
				jdbcProps.put(JDBC_PASSWORD_STR, password);

				/*
				 * to test JDBC driver class
				 */
				try {
					sds = new SimpleDataSource(jdbcProps);
				} catch (Exception e) {
					LOG.warning("Caught Exception while testing driver class name: "
							+ "\n" + e.toString());
					message = res.getString(TEST_DRIVER_CLASS);
					problemFields.add(DRIVER_CLASS_NAME);
				}
			}
			result = sds != null;

			return result;
		}

		private boolean testDBConnectivity() {

			/*
			 * below if block is for testing connection with the database with
			 * given values of input parameters.
			 */
			if (sds != null) {
				try {
					conn = sds.getConnection();
				} catch (SQLException e) {
					LOG.warning("Caught SQLException while testing connection: "
							+ "\n" + e.toString());
					message = res.getString(TEST_CONNECTIVITY);
					problemFields.add(DRIVER_CLASS_NAME);
					problemFields.add(LOGIN);
					problemFields.add(PASSWORD);
					problemFields.add(CONNECTION_URL);
				}
			}

			result = conn != null;

			return result;
		}

		private boolean validateSQLCrawlQuery() {

			/*
			 * Block to test SQL query. SQL query should be of type SELECT, it
			 * should not be DML statement.
			 */
			if (conn != null) {
				try {
					conn.setAutoCommit(false);
					conn.setReadOnly(true);
					stmt = conn.createStatement();
					stmt.setMaxRows(1);
					result = stmt.execute(config.get(SQL_QUERY));
					if (!result) {
						message = res.getString(TEST_SQL_QUERY);
						problemFields.add(SQL_QUERY);
					}
				} catch (SQLException e) {
					LOG.warning("Caught SQLException while testing SQL crawl query : "
							+ "\n" + e.toString());
					message = res.getString(TEST_SQL_QUERY);
					problemFields.add(SQL_QUERY);
				}
			}

			return result;
		}

		/**
		 * @return true if all primary key
		 */

		private boolean validatePrimaryKeyColumns() {
			boolean flag = false;

			try {
				resultSet = stmt.getResultSet();
				if (resultSet != null) {

					ResultSetMetaData rsMeta = resultSet.getMetaData();
					int columnCount = rsMeta.getColumnCount();

					// copy column names
					for (int i = 1; i <= columnCount; i++) {
						String colName = rsMeta.getColumnLabel(i);
						columnNames.add(colName);
					}

					String[] primaryKeys = config.get(PRIMARY_KEYS_STRING).split(",");

					for (String key : primaryKeys) {
						flag = false;
						for (int i = 1; i <= columnCount; i++) {
							if (key.trim().equalsIgnoreCase(rsMeta.getColumnLabel(i))) {
								flag = true;
								break;
							}
						}
						if (!flag) {
							LOG.info("One or more primary keys are invalid");
							message = res.getString(TEST_PRIMARY_KEYS);
							problemFields.add(PRIMARY_KEYS_STRING);
							break;
						}
					}
					if (flag) {
						success = true;
					}
				}
			} catch (SQLException e) {
				LOG.warning("Caught SQLException while testing primary keys: "
						+ "\n" + e.toString());
			}
			return flag;
		}

		/**
		 * This method search for expected placeholders(#username# and $docIds$)
		 * in authZ query and validates authZ query syntax.
		 * 
		 * @param authZQuery authZ query provided by connector admin.
		 * @return true if authZ query has expected placeholders and valid
		 *         syntax.
		 */
		private boolean validateAuthZQuery(String authZQuery) {
			Connection conn = null;
			Statement stmt = null;
			boolean flag = false;
			/*
			 * search for expected placeholders in authZquery.
			 */
			if (authZQuery.contains(USERNAME_PLACEHOLDER)
					&& authZQuery.contains(DOCI_IDS_PLACEHOLDER)) {
				/*
				 * replace placeholders with empty values.
				 */
				authZQuery = authZQuery.replace(USERNAME_PLACEHOLDER, "''");
				authZQuery = authZQuery.replace(DOCI_IDS_PLACEHOLDER, "''");
				try {
					conn = sds.getConnection();
					stmt = conn.createStatement();
					/*
					 * Try to execute authZ query. It will throw an exception if
					 * it is not a valid SQL query.
					 */
					stmt.execute(authZQuery);
					flag = true;
				} catch (Exception e) {
					LOG.warning("Caught SQLException while testing AuthZ query : "
							+ "\n" + e.toString());
					message = res.getString(INVALID_AUTH_QUERY);
					problemFields.add(AUTHZ_QUERY);
				}
				/*
				 * close database connection and statement
				 */
				try {
					if (conn != null) {
						conn.close();
					}
					if (stmt != null) {
						stmt.close();
					}

				} catch (SQLException e) {
					LOG.warning("Caught SQLException " + e.toString());
				}

			} else {
				message = res.getString(INVALID_AUTH_QUERY);
				problemFields.add(AUTHZ_QUERY);
			}

			return flag;
		}

		/**
		 * This method validate the names
		 * 
		 * @return true if external metadata related columns are there SQL crawl
		 *         query.
		 */
		private boolean validateExternalMetadataFields() {

			boolean result = true;

			// validate Document URL field
			String documentURLField = config.get(DOCUMENT_URL_FIELD);

			if (documentURLField != null
					&& documentURLField.trim().length() > 0) {
				if (!columnNames.contains(documentURLField.trim())) {
					result = false;
					message = res.getString(INVALID_COLUMN_NAME);
					problemFields.add(DOCUMENT_URL_FIELD);
				}
			}

			// validate DocID and Base URL fields
			String documentIdField = config.get(DOCUMENT_ID_FIELD);
			String baseURL = config.get(BASE_URL);

			// check if Base URL field exists without DocId Field
			if ((baseURL != null && baseURL.trim().length() > 0)
					&& (documentIdField == null || documentIdField.trim().length() == 0)) {
				result = false;
				message = res.getString(MISSING_ATTRIBUTES) + " : "
						+ res.getString(DOCUMENT_ID_FIELD);
				problemFields.add(DOCUMENT_ID_FIELD);
			}
			// Validate documnet ID column name
			if (documentIdField != null && documentIdField.trim().length() > 0) {

				if (!columnNames.contains(documentIdField)) {
					result = false;
					message = res.getString(INVALID_COLUMN_NAME);
					problemFields.add(DOCUMENT_ID_FIELD);
				}
				if (baseURL == null || baseURL.trim().length() == 0) {
					result = false;
					message = res.getString(MISSING_ATTRIBUTES) + " : "
							+ res.getString(BASE_URL);
					problemFields.add(BASE_URL);
				}

			}

			// validate BLOB/CLOB and Fetch URL field
			String blobClobField = config.get(CLOB_BLOB_FIELD);
			String fetchURL = config.get(FETCH_URL_FIELD);

			// check if Fetch URL field exists without BLOB/CLOB Field
			if ((fetchURL != null && fetchURL.trim().length() > 0)
					&& (blobClobField == null || blobClobField.trim().length() == 0)) {
				result = false;
				message = res.getString(MISSING_ATTRIBUTES) + " : "
						+ res.getString(CLOB_BLOB_FIELD);
				problemFields.add(CLOB_BLOB_FIELD);
			}

			// check for valid BLOB/CLOB column name
			if (blobClobField != null && blobClobField.trim().length() > 0) {
				if (!columnNames.contains(blobClobField)) {
					result = false;
					message = res.getString(INVALID_COLUMN_NAME);
					problemFields.add(CLOB_BLOB_FIELD);
				}

				if (fetchURL != null && fetchURL.trim().length() > 0
						&& !columnNames.contains(fetchURL)) {
					result = false;
					message = res.getString(INVALID_COLUMN_NAME);
					problemFields.add(FETCH_URL_FIELD);
				}
			}

			return result;
		}

		/**
		 *This method validates the name of the document title column
		 * 
		 * @return true if result set contains the document title column entered
		 *         by connector admin, false otherwise.
		 */
		private boolean validateDocTitleField() {
			boolean result = true;
			String docTitleField = config.get(DOC_TITLE_FIELD);
			if (!columnNames.contains(docTitleField)) {
				result = false;
				message = res.getString(INVALID_COLUMN_NAME);
				problemFields.add(DOC_TITLE_FIELD);
			}
			return result;
		}

		/**
		 * This method validates the name of last modified date column
		 * 
		 * @return true if result set contains the last modified date column
		 *         entered by connector admin, false otherwise.
		 */
		private boolean validateLastModifiedField() {
			boolean result = true;
			String lastModifiedDateField = config.get(LAST_MODIFIED_DATE_FIELD);
			if (!columnNames.contains(lastModifiedDateField)) {
				result = false;
				message = res.getString(INVALID_COLUMN_NAME);
				problemFields.add(LAST_MODIFIED_DATE_FIELD);
			}
			return result;
		}

		/**
		 * This method centralizes the calls to different configuration parameter 
		 * validation methods.
		 * @return true if every validation method return true else return false.
		 */
		public boolean validate() {

			password = config.get(PASSWORD);
			login = config.get(LOGIN);
			connectionUrl = config.get(CONNECTION_URL);
			driverClassName = config.get(DRIVER_CLASS_NAME);

			// Test JDBC driver class
			success = testDriverClass();
			if (!success) {
				return success;
			}
			// test Database connectivity
			success = testDBConnectivity();
			if (!success) {
				return success;
			}

			// validate SQL crawl Query
			success = validateSQLCrawlQuery();
			if (!success) {
				return success;
			}

			// validate primary key column names
			success = validatePrimaryKeyColumns();
			if (!success) {
				return success;
			}

			// validate external metadata fields
			success = validateExternalMetadataFields();
			if (!success) {
				return success;
			}

			// validate last modified date column name
			String lastModDateColumn = config.get(LAST_MODIFIED_DATE_FIELD);
			if (lastModDateColumn != null
					&& lastModDateColumn.trim().length() > 0) {
				success = validateLastModifiedField();
				if (!success) {
					return success;
				}
			}

			// validate document title column name
			String docTitleColumn = config.get(DOC_TITLE_FIELD);
			if (docTitleColumn != null && docTitleColumn.trim().length() > 0) {
				success = validateDocTitleField();
				if (!success) {
					return success;
				}
			}

			String authZQuery = config.get(AUTHZ_QUERY);
			/*
			 * validate authZ query if connector admin has provided one.
			 */
			if (authZQuery != null && authZQuery.trim().length() > 0) {
				success = validateAuthZQuery(authZQuery);
			}

			/*
			 * close database connection, result set and statement
			 */
			try {
				if (conn != null) {
					conn.close();
				}
				if (resultSet != null) {
					resultSet.close();
				}
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				LOG.warning("Caught SQLException " + e.toString());
			}

			return success;
		}

		public String getMessage() {
			return message;
		}

		public List<String> getProblemFields() {
			return problemFields;
		}
	}

	/**
	 * Tests if any of the attributes are missing.
	 */
	private class MissingAttributes implements ConfigValidation {
		private Map<String, String> config;
		private String message = "";
		private boolean success = false;
		private List<String> problemFields;
		ResourceBundle res;

		public MissingAttributes(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}

		public String getMessage() {
			return message;
		}

		public List<String> getProblemFields() {
			return problemFields;
		}

		public boolean validate() {
			List<String> missingAttributes = new ArrayList<String>();
			for (Object configKey : configKeys) {
				if (!config.containsKey(configKey)
						&& !configKey.toString().equalsIgnoreCase("externalMetadata")) {
					missingAttributes.add((String) configKey);
				}
			}
			if (missingAttributes.isEmpty()) {
				success = true;
			} else {
				StringBuilder buf = new StringBuilder();
				buf.append(res.getString(MISSING_ATTRIBUTES) + " : ");
				boolean first = true;
				for (String attribute : missingAttributes) {
					if (!first) {
						buf.append(", ");
					}
					first = false;
					buf.append(res.getString(attribute));
				}
				message = buf.toString();
				problemFields = missingAttributes;
			}
			return success;
		}
	}

	/**
	 * Tests if any of the required fields are missing.
	 */
	private static class RequiredFields implements ConfigValidation {
		List<String> missingFields = new ArrayList<String>();
		Map<String, String> config;
		private String message = "";
		private boolean success = false;
		private String[] requiredFields = { HOSTNAME, CONNECTION_URL, DB_NAME,
				LOGIN, DRIVER_CLASS_NAME, SQL_QUERY, PRIMARY_KEYS_STRING };
		private List<String> problemFields;
		ResourceBundle res;

		public RequiredFields(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}

		public String getMessage() {
			return message;
		}

		public List<String> getProblemFields() {
			return problemFields;
		}

		public boolean validate() {
			for (String field : requiredFields) {
				if (config.get(field).equals("")) {
					missingFields.add(field);
				}
			}
			if (missingFields.isEmpty()) {
				success = true;
			} else {
				StringBuilder buf = new StringBuilder();
				buf.append(res.getString(REQ_FIELDS) + " : ");
				boolean first = true;
				for (String attribute : missingFields) {
					if (!first) {
						buf.append(", ");
					}
					first = false;
					buf.append(res.getString(attribute));
				}
				message = buf.toString();
				problemFields = missingFields;
			}
			return success;
		}
	}
	/**
	 * Checks if the Document Title field is entered in the configuration form and also provided in the XSLT,
	 * if present in both the places then shows an error message .
	 * 
	 * Checks if  record Title elements are selected in the XSLT , if present then the Title value needs to be 
	 * indexed appropriately ,else an error message is shown.
	 */
	private static class XSLTCheck implements ConfigValidation
	{
		Map<String, String> config;
		private String message = "";
		private boolean success = false;
		private List<String> problemFields = new ArrayList<String>();
		StringBuilder xslt;
		ResourceBundle res;
		public XSLTCheck(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}
		
		public String getMessage() {
			return message;
		}

		public List<String> getProblemFields() {
			return problemFields;
		}

		public boolean validate() {
			xslt = new StringBuilder (config.get(XSLT));
			if(xslt!=null)
			{
				String XSLT_RECORD_TITLE_ELEMENT;
				int index3;
				
				XSLT_RECORD_TITLE_ELEMENT="<td><xsl:value-of select=\"title";
				index3=xslt.indexOf(XSLT_RECORD_TITLE_ELEMENT);
				
				if(!config.get(DOC_TITLE_FIELD).equals("")&&index3!=-1)
				{
					success=false;
					message = res.getString("XSLT_DOCUMENT_TITLE");
					return success;
				}
				else
				{
					
					String XSLT_RECORD_TITLE_ELEMENT2;
					int index2;
					XSLT_RECORD_TITLE_ELEMENT2="<td><xsl:value-of select=\"title\"/>";
					index2=xslt.indexOf(XSLT_RECORD_TITLE_ELEMENT2);
				
					if(index2!=-1)
					{
						success=false;
						message = res.getString("XSLT_VALIDATE");
						problemFields.add(XSLT);
					}
					else
					{
					success=true;
					}
				}
			}
			return success;
		}
		
	}
	
	/**
	 * Validation Class to check whether HostName is valid. 
	 * 
	 */
	private static class HostNameFQDNCheck implements ConfigValidation {
		Map<String, String> config;
		private String message = "";
		private boolean success = false;
		private List<String> problemFields = new ArrayList<String>();
		String hostName;
		ResourceBundle res;

		public HostNameFQDNCheck(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}

		public String getMessage() {
			return message;
		}

		public List<String> getProblemFields() {
			return problemFields;
		}

		public boolean validate() {
			hostName = config.get(HOSTNAME);
			if (hostName.contains(".")) {
				success = true;
			} else {
				message = res.getString(FQDN_HOSTNAME);
				problemFields.add(HOSTNAME);
			}
			return success;
		}
	}

	
	public ConfigValidation validate(Map<String, String> config,
		ResourceBundle resource) {
		boolean success = false;
		
		ConfigValidation configValidation = new MissingAttributes(config,
				resource);
		success = configValidation.validate();
		if (success) {
			configValidation = new RequiredFields(config, resource);
			success = configValidation.validate();
			if (success) {
				configValidation = new TestDbFields(config, resource);
				success = configValidation.validate();
				if (success) {
					configValidation = new HostNameFQDNCheck(config, resource);
					success = configValidation.validate();
					
					if (success) {
						configValidation = new XSLTCheck(config, resource);
						success = configValidation.validate();
					
					
							if (success) {
								return configValidation;
							}
					}			
				}
			}
		}
		
		return configValidation;
	}

	

	
	}
