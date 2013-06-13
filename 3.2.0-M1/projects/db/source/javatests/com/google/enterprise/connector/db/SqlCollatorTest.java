// Copyright 2013 Google Inc.
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

public class SqlCollatorTest extends DBTestBase {

  // Specify ORDER BY DESC to make sure my SQL query is used,
  // rather than the default collator.
  private static final String COLLATION_QUERY =
      "SELECT Name FROM (SELECT '${source}' AS Name UNION "
      + "SELECT '${target}') AS Temp ORDER BY Name DESC";

  private static final String COLLATION_ID = "Latin1_CI_AI";

  private DBContext dbContext;
  private DBClient dbClient;
  private SqlCollator dbCollator;

  public void setUp() throws Exception {
    super.setUp();
    dbContext = getDbContext();
    dbClient = getDbClient();
    dbCollator = new SqlCollator(dbClient);
    dbContext.setCollator(dbCollator);
  }

  public void testSetGetCollationId() {
    dbCollator.setCollationId(COLLATION_ID);
    assertEquals(COLLATION_ID, dbCollator.getCollationId());
  }

  public void testSetGetCollationQuery() {
    dbCollator.setCollationQuery(COLLATION_QUERY);
    assertEquals(COLLATION_QUERY, dbCollator.getCollationQuery());
  }

  public void testSetBothCollationIdAndQuery() {
    dbCollator.setCollationQuery(COLLATION_QUERY);
    dbCollator.setCollationId(COLLATION_ID);
    assertEquals(COLLATION_QUERY, dbCollator.getCollationQuery());
  }

  public void testGenerateCollationQueriesNullCollationId() throws Exception {
    String result = dbClient.generateCollationQueries(dbCollator);
    assertContains(result, "oracle");
    assertNotContains(result, "NLSSORT");
    assertNotContains(result, "COLLATE");
  }

  public void testGenerateCollationQueriesWithCollationId() throws Exception {
    dbCollator.setCollationId(COLLATION_ID);
    String result = dbClient.generateCollationQueries(dbCollator);
    assertContains(result, COLLATION_ID);
    assertContains(result, "oracle");
    assertContains(result, "NLSSORT");
    assertContains(result, "COLLATE");
  }

  public void testGenerateCollationQueriesWithCustomQuery() throws Exception {
    dbCollator.setCollationQuery(COLLATION_QUERY);
    String result = dbClient.generateCollationQueries(dbCollator);
    assertContains(result, COLLATION_QUERY);
    assertNotContains(result, "oracle");
  }

  public void testSqlCollator() throws Exception {
    dbCollator.setCollationQuery(COLLATION_QUERY);
    dbClient.setDBContext(dbContext);

    assertEquals(0, dbCollator.compare("apple", "apple"));

    // Our custom collation query sorts DESCENDING.
    assertEquals(1, dbCollator.compare("apple", "banana"));
    assertEquals(-1, dbCollator.compare("banana", "apple"));
  }

  private void assertContains(String result, String test) {
    assertTrue(result, result.contains(test));
  }

  private void assertNotContains(String result, String test) {
    assertFalse(result, result.contains(test));
  }

  /* These tests require configuration to external Oracle and SQL Server
   * databases.  Copy the respective JDBC driver JAR files into the
   * third-party/tests directory, then set the DataSource URL, username,
   * and password for the databases.
   * TODO: Get these from the build.properties file.
   */
  static final String oracleDbUrl = null;
  static final String oracleDbUser = null;
  static final String oracleDbPassword = null;
  static final String sqlserverDbUrl = null;
  static final String sqlserverDbUser = null;
  static final String sqlserverDbPassword = null;

  static final int count = 100000;
  static final String string1 = "apple";
  static final String string2 = "banana";

  public void testStringCompareToPerformance() {
    long start = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      string1.compareTo(string2);
    }
    System.out.println(getName() + ": Elapsed time: "
                       + (System.currentTimeMillis() - start) + " ms");
  }

  public void testJavaTextCollatorPerformance() {
    java.text.Collator collator = java.text.Collator.getInstance();
    long start = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      collator.compare(string1, string2);
    }
    System.out.println(getName() + ": Elapsed time: "
                       + (System.currentTimeMillis() - start) + " ms");
  }

  public void testLocalH2CollatorPerformance() throws Exception {
    testSqlCollatorPerformance(null, null, null, null, null, COLLATION_QUERY);
  }

  public void testSqlServerCollatorPerformance() throws Exception {
    if (sqlserverDbUrl != null) {
      testSqlCollatorPerformance("com.microsoft.sqlserver.jdbc.SQLServerDriver",
          sqlserverDbUrl, sqlserverDbUser, sqlserverDbPassword, null, null);
    }
  }

  public void testOracleCollatorPerformance() throws Exception {
    if (oracleDbUrl != null) {
      testSqlCollatorPerformance("oracle.jdbc.OracleDriver",
          oracleDbUrl, oracleDbUser, oracleDbPassword, null, null);
    }
  }

  private void testSqlCollatorPerformance(String driver, String url,
        String user, String password, String collationId,
        String collationQuery) throws Exception {
    configureSqlCollator(driver, url, user, password, collationId,
                         collationQuery);
    long start = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      dbCollator.compare(string1, string2);
    }
    System.out.println(getName() + ": Elapsed time: "
                       + (System.currentTimeMillis() - start) + " ms");
  }

  private void configureSqlCollator(String driver, String url, String user,
        String password, String collationId, String collationQuery)
        throws Exception {
    if (driver != null) {
      dbContext.setDriverClassName(driver);
      dbContext.setConnectionUrl(url);
      dbContext.setLogin(user);
      dbContext.setPassword(password);
    }
    if (collationQuery != null) {
      dbCollator.setCollationQuery(collationQuery);
    } else if (collationId != null) {
      dbCollator.setCollationId(collationId);
    }
    dbClient.setDBContext(dbContext);
  }

  public void testSqlServerSqlCollatorNoCollationId() throws Exception {
    if (sqlserverDbUrl != null) {
      testSqlCollator("com.microsoft.sqlserver.jdbc.SQLServerDriver",
          sqlserverDbUrl, sqlserverDbUser, sqlserverDbPassword, null, null);
    }
  }

  public void testSqlServerSqlCollatorWithCollationId() throws Exception {
    if (sqlserverDbUrl != null) {
      testSqlCollator("com.microsoft.sqlserver.jdbc.SQLServerDriver",
          sqlserverDbUrl, sqlserverDbUser, sqlserverDbPassword,
          "SQL_Latin1_General_CP1_CI_AS", null);
      assertEquals(0, dbCollator.compare(string1, string1.toUpperCase()));
    }
  }

  public void testSqlServerSqlCollatorWithCollationQuery() throws Exception {
    if (sqlserverDbUrl != null) {
      testSqlCollator("com.microsoft.sqlserver.jdbc.SQLServerDriver",
          sqlserverDbUrl, sqlserverDbUser, sqlserverDbPassword, null,
          "SELECT Name FROM (SELECT '${source}' AS Name UNION "
          + "SELECT '${target}') AS Temp ORDER BY Name");
    }
  }

  public void testOracleSqlCollatorNoCollationId() throws Exception {
    if (oracleDbUrl != null) {
      testSqlCollator("oracle.jdbc.OracleDriver",
          oracleDbUrl, oracleDbUser, oracleDbPassword, null, null);
    }
  }

  public void testOracleSqlCollatorWithCollationId() throws Exception {
    if (oracleDbUrl != null) {
      testSqlCollator("oracle.jdbc.OracleDriver",
          oracleDbUrl, oracleDbUser, oracleDbPassword,
          "Latin1_CI", null);
      assertEquals(0, dbCollator.compare(string1, string1.toUpperCase()));
    }
  }

  public void testOracleSqlCollatorWithCollationQuery() throws Exception {
    if (oracleDbUrl != null) {
      testSqlCollator("oracle.jdbc.OracleDriver",
          oracleDbUrl, oracleDbUser, oracleDbPassword, null,
          "SELECT Name FROM (SELECT '${source}' AS Name FROM dual UNION "
          + "SELECT '${target}' FROM dual) Temp ORDER BY Name");
    }
  }

  private void testSqlCollator(String driver, String url, String user,
      String password, String collationId, String collationQuery)
      throws Exception {
    configureSqlCollator(driver, url, user, password, collationId,
                         collationQuery);
    assertEquals(0, dbCollator.compare(string1, string1));
    assertTrue(dbCollator.compare(string1, string2) < 0);
    assertTrue(dbCollator.compare(string2, string1) > 0);
  }
}
