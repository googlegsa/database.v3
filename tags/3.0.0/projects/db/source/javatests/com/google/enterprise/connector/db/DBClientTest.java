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

import com.google.enterprise.connector.spi.RepositoryException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;

public class DBClientTest extends DBTestBase {

  /* @Override */
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
  }

  public void testDBClient() {
    try {
      DBClient dbClient = getDbClient();
      assertNotNull(dbClient);
    } catch (RepositoryException e) {
      fail("Repository Exception in testDBClient");
    }
  }

  /* @Override */
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testConnectivity() {
    Connection connection;
    try {
      connection =
          getDbClient().getSqlMapClient().getDataSource().getConnection();
      assertNotNull(connection);
    } catch (SQLException e) {
      fail("SQL Exception in testConnectivity");
    } catch (RepositoryException e) {
      fail("Repository Exception in testConnectivity");
    }
  }

  /**
   * Test Case when SQL Crawl Query Contains XML reserved symbols(<,>) when
   * IbatisSQLMap is generated with CDATA section for crawl query.
   */
  public void testDBClientWithCDATA() {
    String sqlQuery = "SELECT * FROM TestEmpTable where id < 15";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    try {
      DBClient dbClient = new DBClient(dbContext);
    } catch (DBException e) {
      fail("Failed to initialize DBClient" + e);
    } catch (RuntimeException e) {
      fail("Failed to Initialize DBClient" + e);
    }
  }

  /**
   * Test Case when SQL Crawl Query Contains XML reserved symbols(<,>) when
   * IbatisSQLMap is generated without CDATA section for crawl query.
   */
  public void testDBClientWithoutCDATA() {
    String sqlQuery = "SELECT * FROM TestEmpTable where id < 15";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    String sqlMap = generateIbatisSqlMap(dbContext);
    try {
      new DBClient(dbContext, sqlMap);
      fail("Exception expected XML is not well formed");
    } catch (DBException e) {
      fail("Failed to Initialize DBClient" + e);
    } catch (RuntimeException e) {
      assertEquals("XML is not well formed", e.getMessage());
    }
  }

  /**
   * Method to generate IbatisSQLMAp without CDATA section.
   */
  private String generateIbatisSqlMap(DBContext dbContext) {
    String sqlMap = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
        + "<!DOCTYPE sqlMap "
        + "PUBLIC \"-//ibatis.apache.org//DTD SQL Map 2.0//EN\" "
        + "\"http://ibatis.apache.org/dtd/sql-map-2.dtd\">\n"
        + "<sqlMap namespace=\"IbatisDBClient\">\n"
        + " <select id=\"getAll\" resultClass=\"java.util.HashMap\"> \n"
        + dbContext.getSqlQuery() + "\n </select> \n" + " </sqlMap> \n";
    File file = new File(dbContext.getGoogleConnectorWorkDir(),
        "IbatisSqlMap.xml");
    Writer output;
    try {
      output = new BufferedWriter(new FileWriter(file));
      output.write(sqlMap);
      output.close();
    } catch (IOException e) {
      System.out.println("Cannot write to IbatisSQLMap.xml \n" + e);
    }
    return sqlMap;
  }
}
