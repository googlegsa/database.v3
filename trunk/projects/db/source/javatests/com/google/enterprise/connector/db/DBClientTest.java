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
import java.util.List;
import java.util.Map;

public class DBClientTest extends DBTestBase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testDBClient() {
    DBClient dbClient = getDbClient();
    assertNotNull(dbClient);
  }

  public void testConnectivity() throws Exception {
    Connection connection = getDbClient().getSqlSession().getConnection();
    assertNotNull(connection);
    connection.close();
  }

  /**
   * Test Case when SQL Crawl Query Contains XML reserved symbols(<,>) when
   * IbatisSQLMap is generated with CDATA section for crawl query.
   */
  public void testDBClientWithCDATA() {
    String sqlQuery = "SELECT * FROM TestEmpTable where id < 15";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    DBClient dbClient = dbContext.getClient();
  }

  /**
   * Test Case when SQL Crawl Query Contains XML reserved symbols(<,>) when
   * IbatisSQLMap is generated without CDATA section for crawl query.
   */
  public void testDBClientWithoutCDATA() {
    String sqlQuery = "SELECT * FROM TestEmpTable where id < 15";
    DBContext dbContext = getDbContext();
    dbContext.setSqlQuery(sqlQuery);
    generateIbatisSqlMap(dbContext);
    try {
      new DBClient(dbContext);
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
  private void generateIbatisSqlMap(DBContext dbContext) {
    String sqlMap = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
        + "<!DOCTYPE mapper "
        + "PUBLIC \"-//mybatis.org//DTD Mapper 3.0//EN\" "
        + "\"http://mybatis.org/dtd/mybatis-3-mapper.dtd\">\n"
        + "<mapper namespace=\"IbatisDBClient\">\n"
        + "  <select id=\"getAll\" resultType=\"java.util.HashMap\">\n"
        + dbContext.getSqlQuery() + "\n </select>\n" + "</mapper>/n";

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
  }

  /**
   * Check that missing table returns no rows, but throws no exceptions.
   */
  public void testMissingTable() throws Exception {
    DBClient dbClient = getDbClient();
    runDBScript(DROP_TEST_DB_TABLE);
    List<Map<String, Object>> results = dbClient.executePartialQuery(0, 100);
    assertNotNull(results);
    assertTrue(results.isEmpty());
  }
}
