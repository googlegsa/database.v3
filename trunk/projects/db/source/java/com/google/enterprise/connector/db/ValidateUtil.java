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

import com.google.common.base.Strings;
import com.google.enterprise.connector.spi.ConnectorType;

import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Validation Classes for {@link ConnectorType}.
 */
public class ValidateUtil {
  private static final Logger LOG =
      Logger.getLogger(DBConnectorType.class.getName());
  private static final String TEST_CONNECTIVITY = "TEST_CONNECTIVITY";
  private static final String TEST_DRIVER_CLASS = "TEST_DRIVER_CLASS";
  private static final String TEST_SQL_QUERY = "TEST_SQL_QUERY";
  private static final String TEST_PRIMARY_KEYS = "TEST_PRIMARY_KEYS";
  private static final String INVALID_COLUMN_NAME = "INVALID_COLUMN_NAME";
  private static final String INVALID_AUTH_QUERY = "INVALID_AUTH_QUERY";
  private static final String MISSING_ATTRIBUTES = "MISSING_ATTRIBUTES";
  private static final String REQ_FIELDS = "REQ_FIELDS";
  private static final String TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER =
      "TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER";
  // Red asterisk for required fields.
  public static final String RED_ASTERISK = "<font color=\"RED\">*</font>";

  public ValidateUtil() {
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
    private final Map<String, String> config;
    private String message = "";
    private List<String> problemFields = new ArrayList<String>();
    private final ResourceBundle res;
    private final List<String> columnNames = new ArrayList<String>();
    private final List<Integer> columnTypes = new ArrayList<Integer>();
    private final List<String> columnClasses = new ArrayList<String>();

    private static final String USERNAME_PLACEHOLDER = "#{username}";
    private static final String DOC_IDS_PLACEHOLDER = "${docIds}";
    private static final String KEY_VALUE_PLACEHOLDER = "#{value}";

    Statement stmt = null;
    Connection conn = null;
    UnpooledDataSource dataSource = null;

    public TestDbFields(Map<String, String> config, ResourceBundle res) {
      this.config = config;
      this.res = res;
    }

    private boolean testDriverClass() {
      if (driverClassName != null && connectionUrl != null && login != null
          && password != null) {
        /*
         * to test JDBC driver class
         */
        try {
          dataSource = new UnpooledDataSource(driverClassName, connectionUrl,
                                              login, password);
        } catch (Exception e) {
          LOG.log(Level.WARNING,
                  "Caught Exception while testing driver class name", e);
          message = res.getString(TEST_DRIVER_CLASS);
          problemFields.add(DBConnectorType.DRIVER_CLASS_NAME);
        }
      }
      return dataSource != null;
    }

    private boolean testDBConnectivity() {
      // Test connection with the database with the given input parameters.
      if (dataSource != null) {
        try {
          conn = dataSource.getConnection();
        } catch (SQLException e) {
          LOG.log(Level.WARNING, "Caught SQLException while testing connection",
                  e);
          message = res.getString(TEST_CONNECTIVITY);
          problemFields.add(DBConnectorType.DRIVER_CLASS_NAME);
          problemFields.add(DBConnectorType.LOGIN);
          problemFields.add(DBConnectorType.PASSWORD);
          problemFields.add(DBConnectorType.CONNECTION_URL);
        }
      }
      return conn != null;
    }

    private boolean validateSQLCrawlQuery() {
      // Test SQL query. SQL query should be of type SELECT, it should
      // not be DML statement.
      boolean result = false;
      if (conn != null) {
        try {
          conn.setReadOnly(true);
          conn.setAutoCommit(false);
          stmt = conn.createStatement();
          stmt.setMaxRows(1);

          // TODO(jlacey): This and the authZ test should use the
          // DBClient methods to execute the query, for proper
          // placeholder replacement. That has to happen after the
          // ConnectorFactory is used to create a connector instance
          // and initialize DBClient.
          String sqlQuery = config.get(DBConnectorType.SQL_QUERY);
          if (sqlQuery.contains(KEY_VALUE_PLACEHOLDER)) {
            sqlQuery = sqlQuery.replace(KEY_VALUE_PLACEHOLDER, "0");
          }
          result = executeQueryAndRollback(stmt, sqlQuery, TEST_SQL_QUERY,
              DBConnectorType.SQL_QUERY);
        } catch (SQLException e) {
          LOG.log(Level.WARNING,
                  "Caught SQLException while testing SQL crawl query", e);
          message = res.getString(TEST_SQL_QUERY);
          problemFields.add(DBConnectorType.SQL_QUERY);
        }
      }
      return result;
    }

    /**
     * Executes a query for validation, and rollback the active
     * transaction at the end (for success or failure).
     *
     * @return true if the query returns a ResultSet, or false if the
     *     query throws an exception or does not return a result set
     *     (e.g., an UPDATE statement)
     */
    private boolean executeQueryAndRollback(Statement stmt, String query,
        String messageKey, String fieldId) {
      boolean result = false;
      try {
        try {
          result = stmt.execute(query);
        } catch (SQLException e) {
          LOG.log(Level.WARNING, "Caught SQLException while testing "
              + res.getString(messageKey), e);
        }
        if (!result) {
          message = res.getString(messageKey);
          problemFields.add(fieldId);
        }
      } finally {
        try {
          conn.rollback();
        } catch (Exception e) {
          LOG.log(Level.WARNING,
              "Caught Exception while rolling back transaction", e);
        }
      }
      return result;
    }

    /**
     * @return true if all primary key
     */
    private boolean validatePrimaryKeyColumns() {
      boolean result = false;
      try {
        ResultSet resultSet = stmt.getResultSet();
        if (resultSet != null) {
          // Copy column names.
          try {
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
              columnNames.add(rsMeta.getColumnLabel(i));
              columnTypes.add(rsMeta.getColumnType(i));
              columnClasses.add(rsMeta.getColumnClassName(i));
            }
          } finally {
            resultSet.close();
          }

          String primaryKeys = config.get(DBConnectorType.PRIMARY_KEYS_STRING);
          List<String> matchedColumns =
              Util.getCanonicalPrimaryKey(primaryKeys, columnNames);
          result = (matchedColumns != null);
          if (result) {
            config.put(DBConnectorType.PRIMARY_KEYS_STRING,
                Util.PRIMARY_KEY_JOINER.join(matchedColumns));
          } else {
            LOG.info("One or more primary keys are invalid");
            message = res.getString(TEST_PRIMARY_KEYS);
            problemFields.add(DBConnectorType.PRIMARY_KEYS_STRING);
          }
        }
      } catch (SQLException e) {
        LOG.log(Level.WARNING, "Caught SQLException while testing primary keys",
                e);
      }
      return result;
    }

    /**
     * Searches for expected placeholders (#{username} and ${docIds}) in
     * the AuthZ query and validates the AuthZ query syntax.
     *
     * @param authZQuery AuthZ query provided by connector admin.
     * @return true if AuthZ query has expected placeholders and valid syntax.
     */
    private boolean validateAuthZQuery(String authZQuery) {
      boolean result = false;

      // Search for expected placeholders in authZquery.
      if (authZQuery.contains(USERNAME_PLACEHOLDER)
          && authZQuery.contains(DOC_IDS_PLACEHOLDER)) {
        // Replace placeholders with empty values.
        authZQuery = authZQuery.replace(USERNAME_PLACEHOLDER, "''");
        authZQuery = authZQuery.replace(DOC_IDS_PLACEHOLDER, "''");
        result = executeQueryAndRollback(stmt, authZQuery,
            INVALID_AUTH_QUERY, DBConnectorType.AUTHZ_QUERY);
      } else {
        message = res.getString(INVALID_AUTH_QUERY);
        problemFields.add(DBConnectorType.AUTHZ_QUERY);
      }

      return result;
    }

    /**
     * If a given property is specified, verifies that the value
     * matches one of the database column names, ignoring case, and
     * overwrites the property value in the configuration map with the
     * correct case.
     *
     * @return true if the propery value is valid
     */
    private boolean validateFieldName(String propertyName, String messageKey,
        boolean isRequired) {
      String fieldName = config.get(propertyName);
      if (!Util.isNullOrWhitespace(fieldName)) {
        int index = Util.indexOfIgnoreCase(columnNames, fieldName.trim());
        if (index == -1) {
          message = res.getString(messageKey);
          problemFields.add(propertyName);
          return false;
        } else {
          config.put(propertyName, columnNames.get(index));
        }
      } else if (isRequired) {
        message = res.getString(MISSING_ATTRIBUTES) + ": "
            + res.getString(propertyName);
        problemFields.add(propertyName);
        return false;
      }
      return true;
    }

    /**
     * Validate the metadata property names.
     *
     * @return true if external metadata related columns are valid
     */
    private boolean validateExternalMetadataFields() {
      boolean result;

      // Note: This is not the implied value but the actual value from
      // the submitted form.
      String extMetadataType = config.get(DBConnectorType.EXT_METADATA_TYPE);

      // Validate Document URL field.
      result = validateFieldName(DBConnectorType.DOCUMENT_URL_FIELD,
          INVALID_COLUMN_NAME,
          DBConnectorType.COMPLETE_URL.equals(extMetadataType));

      // Validate Base URL field.
      String baseURL = config.get(DBConnectorType.BASE_URL);
      if (DBConnectorType.DOC_ID.equals(extMetadataType)
          && Util.isNullOrWhitespace(baseURL)) {
        result = false;
        message = res.getString(MISSING_ATTRIBUTES) + ": "
            + res.getString(DBConnectorType.BASE_URL);
        problemFields.add(DBConnectorType.BASE_URL);
      }

      // Validate document ID column name.
      if (!validateFieldName(DBConnectorType.DOCUMENT_ID_FIELD,
          INVALID_COLUMN_NAME,
          DBConnectorType.DOC_ID.equals(extMetadataType))) {
        result = false;
      }

      // Validate BLOB/CLOB and Fetch URL field.
      String blobClobField = config.get(DBConnectorType.CLOB_BLOB_FIELD);
      if (!Util.isNullOrWhitespace(blobClobField)) {
        int index = Util.indexOfIgnoreCase(columnNames, blobClobField.trim());
        if (index == -1) {
          result = false;
          message = res.getString(INVALID_COLUMN_NAME);
          problemFields.add(DBConnectorType.CLOB_BLOB_FIELD);
        } else {
          config.put(DBConnectorType.CLOB_BLOB_FIELD, columnNames.get(index));
          LOG.log(Level.CONFIG,
              "BLOB or CLOB column {0} type is {1}, class name {2}.",
              new Object[] { columnNames.get(index), columnTypes.get(index),
                             columnClasses.get(index) });
        }

        if (!validateFieldName(DBConnectorType.FETCH_URL_FIELD,
            INVALID_COLUMN_NAME, false)) {
          result = false;
        }
      } else if (DBConnectorType.BLOB_CLOB.equals(extMetadataType)) {
        result = false;
        message = res.getString(MISSING_ATTRIBUTES) + ": "
            + res.getString(DBConnectorType.CLOB_BLOB_FIELD);
        problemFields.add(DBConnectorType.CLOB_BLOB_FIELD);
      }

      return result;
    }

    /**
     * Centralizes the calls to different configuration parameter
     * validation methods.
     *
     * @return true if every validation method return true else return false.
     */
    @Override
    public boolean validate() {
      password = config.get(DBConnectorType.PASSWORD);
      login = config.get(DBConnectorType.LOGIN);
      connectionUrl = config.get(DBConnectorType.CONNECTION_URL);
      driverClassName = config.get(DBConnectorType.DRIVER_CLASS_NAME);

      // Test JDBC driver class.
      boolean success = testDriverClass();
      if (!success) {
        return success;
      }

      // Test Database connectivity.
      success = testDBConnectivity();
      if (!success) {
        return success;
      }

      // Validate SQL crawl Query.
      success = validateSQLCrawlQuery();
      if (!success) {
        return success;
      }

      // Validate primary key column names.
      success = validatePrimaryKeyColumns();
      if (!success) {
        return success;
      }

      // Validate external metadata fields.
      success = validateExternalMetadataFields();
      if (!success) {
        return success;
      }

      // Validate last modified date column name.
      success = validateFieldName(DBConnectorType.LAST_MODIFIED_DATE_FIELD,
          INVALID_COLUMN_NAME, false);
      if (!success) {
        return success;
      }

      String authZQuery = config.get(DBConnectorType.AUTHZ_QUERY);
      // Validate authZ query if connector admin has provided one.
      if (!Util.isNullOrWhitespace(authZQuery)) {
        success = validateAuthZQuery(authZQuery);
      }

      // Close database connection, result set and statement.
      try {
        try {
          if (stmt != null) {
            stmt.close();
          }
        } finally {
          if (conn != null) {
            conn.close();
          }
        }
      } catch (SQLException e) {
        LOG.log(Level.WARNING, "Caught SQLException closing connection", e);
      }

      return success;
    }

    @Override
    public String getMessage() {
      return message;
    }

    @Override
    public List<String> getProblemFields() {
      return problemFields;
    }
  }

  /**
   * Tests if any of the required fields are missing.
   */
  private static class RequiredFields implements ConfigValidation {
    private final Map<String, String> config;
    private String message = "";
    private boolean success = false;
    private List<String> problemFields;
    private final ResourceBundle res;

    public RequiredFields(Map<String, String> config, ResourceBundle res) {
      this.config = config;
      this.res = res;
    }

    @Override
    public String getMessage() {
      return message;
    }

    @Override
    public List<String> getProblemFields() {
      return problemFields;
    }

    @Override
    public boolean validate() {
      List<String> missingFields = new ArrayList<String>();
      for (String field : DBConnectorType.ALWAYS_REQUIRED_FIELDS) {
        // TODO(jlacey): Util.isNullOrWhitespace with tests.
        if (config.get(field).equals("")) {
          missingFields.add(field);
        }
      }
      if (missingFields.isEmpty()) {
        success = true;
      } else {
        StringBuilder buf = new StringBuilder();
        buf.append(res.getString(REQ_FIELDS) + ": ");
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
   * Checks if the Document Title field is entered in the configuration form
   * and also provided in the XSLT. If present in both the places then shows an
   * error message. Checks if record Title elements are selected in the XSLT.
   * If present then the Title value needs to be indexed appropriately, else an
   * error message is shown.
   */
  private static class XSLTCheck implements ConfigValidation {
    private final Map<String, String> config;
    private String message = "";
    private List<String> problemFields = new ArrayList<String>();
    private final ResourceBundle res;

    public XSLTCheck(Map<String, String> config, ResourceBundle res) {
      this.config = config;
      this.res = res;
    }

    @Override
    public String getMessage() {
      return message;
    }

    @Override
    public List<String> getProblemFields() {
      return problemFields;
    }

    @Override
    public boolean validate() {
      boolean success;
      String xslt = Strings.nullToEmpty(config.get(DBConnectorType.XSLT));
      int index = xslt.indexOf("<td><xsl:value-of select=\"title\"/>");
      if (index != -1) {
        success = false;
        message = res.getString("XSLT_VALIDATE");
        problemFields.add(DBConnectorType.XSLT);
      } else {
        success = true;
      }
      return success;
    }
  }

  /**
   * Validation class to check whether Single primary key is used when using
   * parameterized crawl query .
   */
  private static class QueryParameterAndPrimaryKeyCheck implements
      ConfigValidation {
    private final Map<String, String> config;
    private final ResourceBundle res;
    private String message = "";
    private boolean success = false;
    private List<String> problemFields = new ArrayList<String>();
    private static final String KEY_VALUE_PLACEHOLDER = "#{value}";

    public QueryParameterAndPrimaryKeyCheck(Map<String, String> config,
        ResourceBundle res) {
      this.config = config;
      this.res = res;
    }

    @Override
    public String getMessage() {
      return message;
    }

    @Override
    public List<String> getProblemFields() {
      return problemFields;
    }

    @Override
    public boolean validate() {
      // We have already validated the primary key, we just need to
      // see if it has more than one column.
      String primaryKeys = config.get(DBConnectorType.PRIMARY_KEYS_STRING);
      String sqlCrawlQuery = config.get(DBConnectorType.SQL_QUERY);
      if (primaryKeys.indexOf(Util.PRIMARY_KEY_SEPARATOR) != -1
          && sqlCrawlQuery.contains(KEY_VALUE_PLACEHOLDER)) {
        success = false;
        message = res.getString(TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER);
        problemFields.add(DBConnectorType.PRIMARY_KEYS_STRING);
        problemFields.add(DBConnectorType.SQL_QUERY);
      } else {
        success = true;
      }
      return success;
    }
  }

  public ConfigValidation validate(Map<String, String> config,
      ResourceBundle resource) {
    ConfigValidation[] configValidations = {
      new RequiredFields(config, resource),
      new TestDbFields(config, resource),
      new XSLTCheck(config, resource),
      new QueryParameterAndPrimaryKeyCheck(config, resource),
    };
    for (ConfigValidation configValidation : configValidations) {
      if (!configValidation.validate()) {
        return configValidation;
      }
    }
    return null;
  }
}
