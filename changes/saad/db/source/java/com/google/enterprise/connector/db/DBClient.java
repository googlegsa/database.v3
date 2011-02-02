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

import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapClientBuilder;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.sql.DataSource;



/**
 * A client which gets rows from a database corresponding to a given SQL query.
 * <p>
 * It uses IBatis to talk to the database and generates SqlMapConfig and SqlMap
 * required by IBatis.
 */
public class DBClient {

	private static final Logger LOG = Logger.getLogger(DBClient.class.getName());

	private final DBContext dbContext;
	private final SqlMapClient sqlMapClient;
	private final String sqlQuery;
	private final String googleConnectorWorkDir;
	private final String[] primaryKeys;
	private String sqlMapConfig = null;
	private String sqlMap = null;
	private final String authZQuery;

	/**
	 * @param dbContext holds the database context.
	 * @param sqlQuery SQL query to execute on the database.
	 * @param googleConnectorWorkDir working directory of DB connector.
	 * @param primaryKeys primary keys for the result DB table.
	 * @throws DBException
	 */
	public DBClient(DBContext dbContext, String sqlQuery,
			String googleConnectorWorkDir, String[] primaryKeys,
			String authZQuery) throws DBException {
		this.dbContext = dbContext;
		this.sqlQuery = sqlQuery;
		this.authZQuery = authZQuery;
		this.googleConnectorWorkDir = googleConnectorWorkDir;
		this.primaryKeys = primaryKeys;
		LOG.info("Google connector Workdir is"+googleConnectorWorkDir);
		generateSqlMapConfig();
		generateSqlMap();
		InputStream resources;
		try {
			resources = new ByteArrayInputStream(sqlMapConfig.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new DBException("Could not instantiate DBClient.", e);
		}
		this.sqlMapClient = SqlMapClientBuilder.buildSqlMapClient(resources);

		LOG.info("DBClient for database " + getDatabaseInfo()
				+ " is instantiated");

	}

	/**
	 * getter method for sqlMapClient, so that it can be used outside to perform
	 * database related operation
	 * 
	 * @return SqlMapClient that is used to perform database operations like
	 *         CRUD.
	 */

	public SqlMapClient getSqlMapClient() {
		return sqlMapClient;
	}

	/**
	 * @return the googleConnectorWorkDir
	 */
	public String getGoogleConnectorWorkDir() {
		return googleConnectorWorkDir;
	}

	/**
	 * @return primaryKeys
	 */
	public String[] getPrimaryKeys() {
		return primaryKeys;
	}

	/**
	 * @return dbContext
	 */
	public DBContext getDBContext() {
		return dbContext;
	}

	/**
	 * @return rows - result of executing the SQL query. E.g., result table with
	 *         columns id and lastName and two rows will be returned as
	 * 
	 *         <pre>
	 *        [{id=1, lastName=last_01}, {id=2, lastName=last_02}]
	 * </pre>
	 * @throws DBException
	 */
	public List<Map<String, Object>> executeQuery() throws DBException {
		List<Map<String, Object>> rows;
		try {
			rows = sqlMapClient.queryForList("IbatisDBClient.getAll", null);
		} catch (SQLException e) {
			throw new DBException("Could not execute query on the database\n",
					e);
		}
		LOG.info("Rows returned : " + rows);
		return rows;
	}

	/**
	 * @param skipRows number of rows to skip in the database.
	 * @param maxRows max number of rows to return.
	 * @return rows - subset of the result of executing the SQL query. E.g.,
	 *         result table with columns id and lastName and two rows will be
	 *         returned as
	 * 
	 *         <pre>
	 *         [{id=1, lastName=last_01}, {id=2, lastName=last_02}]
	 * </pre>
	 * @throws DBException
	 */
	public List<Map<String, Object>> executePartialQuery(int skipRows,
			int maxRows) throws DBException {
		// TODO(meghna): Think about a better way to scroll through the result
		// set.
		List<Map<String, Object>> rows;
		LOG.info("Executing partial query with skipRows = " + skipRows + "and "
				+ "maxRows = " + maxRows);
		try {
			rows = sqlMapClient.queryForList("IbatisDBClient.getAll", skipRows, maxRows);
		} catch (Exception e) {
			/*
			 * Below code is added to handle scenarios when table is deleted or
			 * connectivity with database is lost. In this scenario connector
			 * first check the connectivity with database and if there is
			 * connectivity it returns empty list of rows else throughs
			 * RepositoryException.
			 */
			DataSource ds = sqlMapClient.getDataSource();
			Connection conn = null;
			try {
				conn = ds.getConnection();
				LOG.info("Could not execute SQL query on the database\n"
						+ e.toString());
				rows = new ArrayList<Map<String, Object>>();
			} catch (SQLException e1) {
				LOG.info("Unable to connect to the database\n" + e.toString());
				throw new DBException(e);
			} finally {
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e1) {
						LOG.info("Could not close database connection: "
								+ e1.toString());
					}
				}
			}
		}
		LOG.info("Rows returned : " + rows);
		return rows;
	}

	/**
	 * Generates the SqlMapConfig for mysql database. It contains a reference to
	 * the SqlMap which should be a url or a file. It assumes that the SqlMap is
	 * in IbatisSqlMap.xml in the googleConnectorWorkDir.
	 */
	private void generateSqlMapConfig() {
		/*
		 * TODO(meghna): Look into <properties resource="
		 * examples/sqlmap/maps/SqlMapConfigExample.properties " /> Also look
		 * into making DTD retrieving local with
		 * "jar:file:<path_to_jar>/dtd.jar!<path_to_dtd>/sql-map-config-2.dtd"
		 */
		sqlMapConfig = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
				+ "<!DOCTYPE sqlMapConfig "
				+ "PUBLIC \"-//ibatis.apache.org//DTD SQL Map Config 2.0//EN\" "
				+ "\"http://ibatis.apache.org/dtd/sql-map-config-2.dtd\">\n"
				+ "<sqlMapConfig>\n"
				+ "<settings useStatementNamespaces=\"true\"/>\n"
				+ "<transactionManager type=\"JDBC\">\n"
				+ " <dataSource type=\"SIMPLE\">\n"
				+ " <property name=\"JDBC.Driver\" value=\""
				+ dbContext.getDriverClassName() + "\" />\n"
				+ " <property name=\"JDBC.ConnectionURL\" value=\""
				+ dbContext.getConnectionUrl() + "\" />\n"
				+ " <property name=\"JDBC.Username\" value=\""
				+ dbContext.getLogin() + "\" />\n"
				+ " <property name=\"JDBC.Password\" value=\""
				+ dbContext.getPassword() + "\" />\n"
				+ " </dataSource></transactionManager>\n"
				+ " <sqlMap url=\"file:///" + googleConnectorWorkDir
				+ "/IbatisSqlMap.xml\"/>\n" + "</sqlMapConfig>\n";

		String oldString = " <property name=\"JDBC.Password\" value=\""
				+ dbContext.getPassword() + "\" />";
		String newString = " <property name=\"JDBC.Password\" value=\""
				+ "*****" + "\" />";

		LOG.config("Generated sqlMapConfig : \n"
				+ sqlMapConfig.replace(oldString, newString));
	}

	/**
	 * Generates the SqlMap which contains the SQL query. It writes the SqlMap
	 * in IbatisSqlMap.xml under googleConnectorWorkDir.
	 * 
	 * @throws DBException
	 */
	private void generateSqlMap() throws DBException {
		/*
		 * TODO(meghna): Look into making DTD retrieving local with
		 * "jar:file:<path_to_jar>/dtd.jar!<path_to_dtd>/sql-map-config-2.dtd"
		 */
		sqlMap = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
				+ "<!DOCTYPE sqlMap "
				+ "PUBLIC \"-//ibatis.apache.org//DTD SQL Map 2.0//EN\" "
				+ "\"http://ibatis.apache.org/dtd/sql-map-2.dtd\">\n"
				+ "<sqlMap namespace=\"IbatisDBClient\">\n"
				+ " <select id=\"getAll\" resultClass=\"java.util.HashMap\"> \n"
				+ sqlQuery + "\n </select> \n";

		/*
		 * check if authZ query is provided. If authZ query is there , add
		 * 'select' element for getting authorized documents.
		 */
		if (authZQuery != null && authZQuery.trim().length() > 0) {
			sqlMap = sqlMap
					+ "<select id=\"getAuthorizedDocs\"  parameterClass=\"java.util.HashMap\"  resultClass=\"java.lang.String\"> \n "
					+ authZQuery + "</select>";

			dbContext.setPublicFeed(false);
		} else {
			dbContext.setPublicFeed(true);
		}
		// close 'sqlMap' element
		sqlMap = sqlMap + " </sqlMap> \n";

		LOG.config("Generated sqlMap : \n" + sqlMap);
		File file = new File(googleConnectorWorkDir, "IbatisSqlMap.xml");
		Writer output;
		try {
			output = new BufferedWriter(new FileWriter(file));
			output.write(sqlMap);
			output.close();
		} catch (IOException e) {
			throw new DBException("Could not write to/close Sql Map "
					+ googleConnectorWorkDir + "/IbatisSqlMap.xml", e);
		}
	}

	/**
	 * This method return the database name and version details.
	 * 
	 * @author Suresh_Ghuge
	 * @return database name and version details
	 */
	public String getDatabaseInfo() {
		String dbDetails = "";
		Connection conn = null;
		DatabaseMetaData meta = null;
		try {
			SqlMapClient sqlClient = getSqlMapClient();
			if (sqlClient != null) {
				conn = sqlClient.getDataSource().getConnection();
				if (conn != null) {
					meta = conn.getMetaData();
					if (meta != null) {
						String productName = meta.getDatabaseProductName();
						String productVersion = meta.getDatabaseProductVersion();
						dbDetails = productName + " " + productVersion;
					}
				}
			}
		} catch (SQLException e) {
			LOG.warning("Caught SQLException while fetching database details"
					+ e.toString());
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					LOG.warning("Caught SQLException while closing connection : "
							+ e.toString());
				}
			}
		}
		return dbDetails;
	}

	/**
	 * This method executes the authZ query for given user-name and list of
	 * documents and returns the list of authorized documents.
	 * 
	 * @param userName user-name
	 * @param docIds List of documents to be authorized
	 * @return list of authorized documents
	 */
	public List<String> executeAuthZQuery(String userName, String docIds) {

		List<String> authorizedDocs = new ArrayList<String>();
		/*
		 * Create a hashmap as to provide input parameters user-name and list of
		 * documents to authZ query.
		 */
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("username", userName);
		paramMap.put("docIds", docIds);
		/*
		 * Execute the authZ query.
		 */
		try {
			authorizedDocs = sqlMapClient.queryForList("IbatisDBClient.getAuthorizedDocs", paramMap);
		} catch (Exception e) {
			LOG.warning("Could not execute AuthZ query on the database\n"
					+ e.getMessage());
		}
		return authorizedDocs;
	}

}
