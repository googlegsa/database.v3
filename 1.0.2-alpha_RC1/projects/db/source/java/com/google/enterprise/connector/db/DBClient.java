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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapClientBuilder;

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

	/**
	 * @param dbContext holds the database context.
	 * @param sqlQuery SQL query to execute on the database.
	 * @param googleConnectorWorkDir working directory of DB connector.
	 * @param primaryKeys primary keys for the result DB table.
	 * @throws DBException
	 */
	public DBClient(DBContext dbContext, String sqlQuery,
			String googleConnectorWorkDir, String[] primaryKeys)
			throws DBException {
		this.dbContext = dbContext;
		this.sqlQuery = sqlQuery;
		this.googleConnectorWorkDir = googleConnectorWorkDir;
		this.primaryKeys = primaryKeys;
		generateSqlMapConfig();
		generateSqlMap();
		InputStream resources;
		try {
			resources = new ByteArrayInputStream(sqlMapConfig.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new DBException("Could not instantiate DBClient.", e);
		}
		this.sqlMapClient = SqlMapClientBuilder.buildSqlMapClient(resources);
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
		} catch (SQLException e) {
			throw new DBException("Could not execute query on the database\n",
					e);
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

		LOG.info("Generated sqlMapConfig : \n"
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
				+ sqlQuery + "\n </select> \n" + " </sqlMap> \n";
		LOG.info("Generated sqlMap : \n" + sqlMap);
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
}
