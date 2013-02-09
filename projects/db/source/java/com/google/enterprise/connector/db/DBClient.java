// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.enterprise.connector.spi.XmlUtils;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A client which gets rows from a database corresponding to a given SQL query.
 * <p>
 * It uses MyBatis to talk to the database.
 */
public class DBClient {
  private static final Logger LOG = Logger.getLogger(DBClient.class.getName());

  protected DBContext dbContext;
  protected SqlSessionFactory sqlSessionFactory;

  static {
    org.apache.ibatis.logging.LogFactory.useJdkLogging();
  }

  public DBClient() {
  }

  /**
   * @param dbContext holds the database context.
   * @param sqlQuery SQL query to execute on the database.
   * @param googleConnectorWorkDir working directory of DB connector.
   * @param primaryKeys primary keys for the result DB table.
   * @throws DBException
   */
  public void setDBContext(DBContext dbContext) throws DBException {
    this.dbContext = dbContext;
    generateSqlMap();
    this.sqlSessionFactory = getSqlSessionFactory(generateMyBatisConfig());
    LOG.info("DBClient for database " + getDatabaseInfo() + " is instantiated");
  }

  /**
   * Constructor used for testing purpose. DBCLient initialized with sqlMap
   * having crawl query without CDATA section.
   */
  @VisibleForTesting
  DBClient(DBContext dbContext) throws DBException {
    this.dbContext = dbContext;
    this.sqlSessionFactory = getSqlSessionFactory(generateMyBatisConfig());
  }

  private SqlSessionFactory getSqlSessionFactory(String config) {
    try {
      SqlSessionFactoryBuilder builder = new SqlSessionFactoryBuilder();
      return builder.build(new StringReader(config));
    } catch (RuntimeException e) {
      throw new RuntimeException("XML is not well formed", e);
    }
  }

  /**
   * @return a SqlSession
   */
  @VisibleForTesting
  SqlSession getSqlSession()
      throws SnapshotRepositoryRuntimeException {
    try {
      return sqlSessionFactory.openSession();
    } catch (RuntimeException e) {
      Throwable cause = (e.getCause() != null &&
          e.getCause() instanceof SQLException) ? e.getCause() : e;
      LOG.log(Level.WARNING, "Unable to connect to the database.", cause);
      throw new SnapshotRepositoryRuntimeException(
          "Unable to connect to the database.", cause);
    }
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
  public List<Map<String, Object>> executePartialQuery(int skipRows, int maxRows)
      throws SnapshotRepositoryRuntimeException {
    // TODO(meghna): Think about a better way to scroll through the result set.
    List<Map<String, Object>> rows;
    LOG.info("Executing partial query with skipRows = " + skipRows + " and "
        + "maxRows = " + maxRows);
    SqlSession session = getSqlSession();
    try {
      rows = session.selectList("IbatisDBClient.getAll", null,
                                new RowBounds(skipRows, maxRows));
      LOG.info("Sucessfully executed partial query with skipRows = "
          + skipRows + " and maxRows = " + maxRows);
    } catch (RuntimeException e) {
      checkDBConnection(session, e);
      rows = new ArrayList<Map<String, Object>>();
    } finally {
      session.close();
    }
    LOG.info("Number of rows returned " + rows.size());
    return rows;
  }

  /**
   * Executes the partial parameterized query for given keyValue and
   * returns the list of records having their key value greater than keyValue
   * parameter.
   *
   * @param keyValue
   * @return list of documents
   */
  public List<Map<String, Object>> executeParameterizePartialQuery(
      Integer keyValue) throws SnapshotRepositoryRuntimeException {
    List<Map<String, Object>> rows;
    int skipRows = 0;
    int maxRows = dbContext.getNumberOfRows();
    // Create a hashmap as to provide input parameters minvalue and maxvalue to
    // the query.
    Map<String, Object> paramMap = new HashMap<String, Object>();
    paramMap.put("value", keyValue);
    LOG.info("Executing partial parametrized query with keyValue = " + keyValue);
    SqlSession session = getSqlSession();
    try {
      rows = session.selectList("IbatisDBClient.getAll", paramMap,
                                new RowBounds(skipRows, maxRows));
      LOG.info("Sucessfully executed partial parametrized query with keyValue = "
          + keyValue);
    } catch (RuntimeException e) {
      checkDBConnection(session, e);
      rows = new ArrayList<Map<String, Object>>();
    } finally {
      session.close();
    }
    LOG.info("Number of rows returned " + rows.size());
    return rows;
  }

  private void checkDBConnection(SqlSession session, Exception e)
      throws SnapshotRepositoryRuntimeException {
    /*
     * Below code is added to handle scenarios when table is deleted or
     * connectivity with database is lost. In this scenario connector first
     * check the connectivity with database and if there is no connectivity,
     * throw a SnapshotRepositoryRuntimeException, otherwise 
     * allow the connector to continue as if there was no data available.
     */
    Connection conn = null;
    try {
      conn = session.getConnection();
      LOG.log(Level.WARNING, "Could not execute SQL query on the database", e);
      // Swallow the exception.
    } catch (RuntimeException e1) {
      Throwable cause = (e1.getCause() != null &&
          e1.getCause() instanceof SQLException) ? e1.getCause() : e1;
      LOG.log(Level.WARNING, "Unable to connect to the database", cause);
      throw new SnapshotRepositoryRuntimeException(
          "Unable to connect to the database", cause);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e1) {
          LOG.fine("Could not close database connection: " + e1.toString());
        }
      }
    }
  }

  /**
   * Generates the SqlMapConfig for mysql database. It contains a reference to
   * the SqlMap which should be a url or a file. It assumes that the SqlMap is
   * in IbatisSqlMap.xml in the googleConnectorWorkDir.
   *
   * @return MyBatis Configuration XML string.
   */
  private String generateMyBatisConfig() {
    /*
     * TODO(meghna): Look into <properties resource="
     * examples/sqlmap/maps/SqlMapConfigExample.properties " /> Also look into
     * making DTD retrieving local with
     * "jar:file:<path_to_jar>/dtd.jar!<path_to_dtd>/mybatis-3-config.dtd"
     */
    String passwordFormat = "<property name=\"password\" value=\"%s\"/>";
    String passwordElem = 
        String.format(passwordFormat, toAttrValue(dbContext.getPassword()));
    String config = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
        + "<!DOCTYPE configuration "
        + "PUBLIC \"-//mybatis.org//DTD Config 3.0//EN\" "
        + "\"http://mybatis.org/dtd/mybatis-3-config.dtd\">\n"
        + "<configuration>\n"
        + "  <typeHandlers>\n"
        + "    <typeHandler jdbcType=\"BLOB\" "
        + "handler=\"com.google.enterprise.connector.db.BlobTypeHandler\"/>\n"
        + "    <typeHandler jdbcType=\"CLOB\" "
        + "handler=\"com.google.enterprise.connector.db.ClobTypeHandler\"/>\n"
        + "  </typeHandlers>\n"
        + "  <environments default=\"connector\">\n"
        + "    <environment id=\"connector\">\n"
        + "      <transactionManager type=\"JDBC\"/>\n"
        + "      <dataSource type=\"POOLED\">\n"
        + "        <property name=\"driver\" value=\""
        + toAttrValue(dbContext.getDriverClassName()) + "\"/>\n"
        + "        <property name=\"url\" value=\""
        + toAttrValue(dbContext.getConnectionUrl()) + "\"/>\n"
        + "        <property name=\"username\" value=\""
        + toAttrValue(dbContext.getLogin()) + "\"/>\n"
        + "        " + passwordElem + "\n"
        + "      </dataSource>\n"
        + "    </environment>\n"
        + "  </environments>\n"
        + "  <mappers>\n"
        + "    <mapper url=\"file:///"
        + toAttrValue(dbContext.getGoogleConnectorWorkDir()
                      + "/IbatisSqlMap.xml") + "\"/>\n"
        + "  </mappers>\n"
        + "</configuration>\n";

    LOG.config("Generated MyBatis Configuration:\n"
        + config.replace(passwordElem, String.format(passwordFormat, "*****")));
    return config;
  }
  
  /** Escapes special characters in value for use in an XML attribute value. */
  private String toAttrValue(String value) {
    StringBuilder builder = new StringBuilder();
    try {
      XmlUtils.xmlAppendAttrValue(value, builder);
    } catch (IOException e) {
      // Can't happen with StringBuilder.
      throw new AssertionError(e);
    }
    return builder.toString();
  }

  /**
   * Generates the SqlMap which contains the SQL query. It writes the SqlMap in
   * IbatisSqlMap.xml under googleConnectorWorkDir.
   *
   * @throws DBException
   */
  private void generateSqlMap() throws DBException {
    /*
     * TODO(meghna): Look into making DTD retrieving local with
     * "jar:file:<path_to_jar>/dtd.jar!<path_to_dtd>/mybatis-3-mapper.dtd"
     */
    /*
     * TODO(bmj): Look into making this resource available as
     * an in-memory resource or Mapper class, to avoid dumping
     * this into the file system.
     */

    /*
     * Use CDATA section for escaping XML reserved symbols as documented on
     * iBatis data mapper developer Guide
     */
    String sqlMap = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
        + "<!DOCTYPE mapper "
        + "PUBLIC \"-//mybatis.org//DTD Mapper 3.0//EN\" "
        + "\"http://mybatis.org/dtd/mybatis-3-mapper.dtd\">\n"
        + "<mapper namespace=\"IbatisDBClient\">\n"
        + "  <select id=\"getAll\" resultType=\"java.util.HashMap\">\n"
        + "    <![CDATA[ " + dbContext.getSqlQuery() + " ]]>\n"
        + "  </select> \n";

    /*
     * check if authZ query is provided. If authZ query is there , add 'select'
     * element for getting authorized documents.
     */
    if (dbContext.getAuthZQuery() != null
        && dbContext.getAuthZQuery().trim().length() > 0) {
      sqlMap += "  <select id=\"getAuthorizedDocs\" parameterType="
          + "\"java.util.HashMap\" resultType=\"java.lang.String\">\n "
          + "    <![CDATA[ " + dbContext.getAuthZQuery()  + " ]]>\n"
          + "  </select>";
      dbContext.setPublicFeed(false);
    } else {
      dbContext.setPublicFeed(true);
    }
    sqlMap += "</mapper>\n";

    LOG.config("Generated sqlMap : \n" + sqlMap);
    File file = new File(dbContext.getGoogleConnectorWorkDir(),
        "IbatisSqlMap.xml");
    Writer output;
    try {
      output = new BufferedWriter(new FileWriter(file));
      output.write(sqlMap);
      output.close();
    } catch (IOException e) {
      throw new DBException("Could not write to/close Sql Map "
          + dbContext.getGoogleConnectorWorkDir() + "/IbatisSqlMap.xml", e);
    }
  }

  /**
   * Like google.common.base.Function, but apply() may
   * throw SQLExceptions.
   */
  public interface SqlFunction<F, T> {
    public T apply(F input) throws SQLException;
  }

  /**
   * Returns the database name and version details.
   *
   * @author Suresh_Ghuge
   * @return database name and version details
   */
  public String getDatabaseInfo() {
    return Strings.nullToEmpty(getDatabaseMetaData(
        new SqlFunction<DatabaseMetaData, String>() {
          public String apply(DatabaseMetaData metaData) throws SQLException {
            return metaData.getDatabaseProductName() + " "
                 + metaData.getDatabaseProductVersion();
          }
        }));
  }

  /**
   * Returns information derived from the DatabaseMetaData.
   *
   * @param metaDataHandler a Function that takes a DatabaseMetaData as input
   *        and returns a value
   * @return the value returned by the metaDataHandler Function, or null if
   *        there was an error
   */
  public <T> T getDatabaseMetaData(
       SqlFunction<DatabaseMetaData, T>  metaDataHandler) {
    try {
      SqlSession session = sqlSessionFactory.openSession();
      try {
        Connection conn = session.getConnection();
        try {
          DatabaseMetaData meta = conn.getMetaData();
          if (meta != null) {
            return metaDataHandler.apply(meta);
          }
        } finally {
          conn.close();
        }
      } finally {
        session.close();
      }
    } catch (SQLException e) {
      LOG.warning("Caught SQLException while fetching database details: " + e);
    } catch (Exception e) {
      LOG.warning("Caught Exception while fetching database details: " + e);
    }
    return null;
  }

  /**
   * Executes the AuthZ query for given user-name and list of
   * documents and returns the list of authorized documents.
   *
   * @param userName user-name
   * @param docIds List of documents to be authorized
   * @return list of authorized documents
   */
  @SuppressWarnings("unchecked")
  public List<String> executeAuthZQuery(String userName, String docIds) {
    List<String> authorizedDocs = new ArrayList<String>();
    // Create a hashmap as to provide input parameters userName and list of
    // documents to AuthZ query.
    Map<String, Object> paramMap = new HashMap<String, Object>();
    paramMap.put("username", userName);
    paramMap.put("docIds", docIds);

    // Execute the AuthZ query.
    SqlSession session = getSqlSession();
    try {
      authorizedDocs = session.selectList(
          "IbatisDBClient.getAuthorizedDocs", paramMap);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Could not execute AuthZ query on the database.",
              e);
    } finally {
      session.close();
    }
    return authorizedDocs;
  }

  /**
   * Returns true if nulls sort low in this database implementation; or
   * false if nulls sort high.
   */
  public Boolean nullsAreSortedLow() {
    return getDatabaseMetaData(
        new SqlFunction<DatabaseMetaData, Boolean>() {
          public Boolean apply(DatabaseMetaData meta) throws SQLException {
            return Boolean.valueOf(meta.nullsAreSortedLow()
                                   || meta.nullsAreSortedAtStart());
          }
        });
  }
}
