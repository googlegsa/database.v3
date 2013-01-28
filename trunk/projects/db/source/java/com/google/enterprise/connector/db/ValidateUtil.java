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

import com.google.enterprise.connector.spi.ConnectorType;

import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.TreeMap;
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
  private static final String FQDN_HOSTNAME = "FQDN_HOSTNAME";
  private static final String MISSING_ATTRIBUTES = "MISSING_ATTRIBUTES";
  private static final String REQ_FIELDS = "REQ_FIELDS";
  private static final String TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER =
      "TEST_PRIMARY_KEYS_AND_KEY_VALUE_PLACEHOLDER";
  private static final String PRIMARY_KEYS_SEPARATOR = ",";
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
    private Map<String, String> config;
    private String message = "";
    private boolean success = false;
    private List<String> problemFields = new ArrayList<String>();
    private ResourceBundle res;
    List<String> columnNames = new ArrayList<String>();

    private static final String USERNAME_PLACEHOLDER = "#{username}";
    private static final String DOCI_IDS_PLACEHOLDER = "${docIds}";
    private static final String KEY_VALUE_PLACEHOLDER = "#{value}";

    Statement stmt = null;
    Connection conn = null;
    ResultSet resultSet = null;
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
          try {
            // TODO(jlacey): This and the authZ test should use the
            // DBClient methods to execute the query, for proper
            // placeholder replacement. That has to happen after the
            // ConnectorFactory is used to create a connector instance
             // and initialize DBClient.
            String sqlQuery = config.get(DBConnectorType.SQL_QUERY);
            if (sqlQuery.contains(KEY_VALUE_PLACEHOLDER)) {
              sqlQuery = sqlQuery.replace(KEY_VALUE_PLACEHOLDER, "0");
              result = stmt.execute(sqlQuery);
            } else {
              result = stmt.execute(sqlQuery);
            }
            if (!result) {
              message = res.getString(TEST_SQL_QUERY);
              problemFields.add(DBConnectorType.SQL_QUERY);
            }
          } finally {
            try {
              conn.rollback();
            } catch (Exception e) {
              LOG.log(Level.WARNING,
                  "Caught Exception while rolling back transaction", e);
            }
          }
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
     * @return true if all primary key
     */
    private boolean validatePrimaryKeyColumns() {
      boolean flag = false;
      try {
        resultSet = stmt.getResultSet();
        if (resultSet != null) {
          ResultSetMetaData rsMeta = resultSet.getMetaData();
          int columnCount = rsMeta.getColumnCount();

          // Copy column names.
          for (int i = 1; i <= columnCount; i++) {
            String colName = rsMeta.getColumnLabel(i);
            columnNames.add(colName);
          }

          String[] primaryKeys = config.get(DBConnectorType.PRIMARY_KEYS_STRING)
              .split(PRIMARY_KEYS_SEPARATOR);
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
              problemFields.add(DBConnectorType.PRIMARY_KEYS_STRING);
              break;
            }
          }
          if (flag) {
            success = true;
          }
        }
      } catch (SQLException e) {
        LOG.log(Level.WARNING, "Caught SQLException while testing primary keys",
                e);
      }
      return flag;
    }

    /**
     * Searches for expected placeholders(#username# and $docIds$) in
     * AuthZ query and validates AuthZ query syntax.
     *
     * @param authZQuery AuthZ query provided by connector admin.
     * @return true if AuthZ query has expected placeholders and valid syntax.
     */
    private boolean validateAuthZQuery(String authZQuery) {
      Connection conn = null;
      Statement stmt = null;
      boolean flag = false;

      // Search for expected placeholders in authZquery.
      if (authZQuery.contains(USERNAME_PLACEHOLDER)
          && authZQuery.contains(DOCI_IDS_PLACEHOLDER)) {
        // Replace placeholders with empty values.
        authZQuery = authZQuery.replace(USERNAME_PLACEHOLDER, "''");
        authZQuery = authZQuery.replace(DOCI_IDS_PLACEHOLDER, "''");
        try {
          conn = dataSource.getConnection();
          stmt = conn.createStatement();
          // Try to execute authZ query. It will throw an exception if it is not
          // a valid SQL query.
          stmt.execute(authZQuery);
          flag = true;
        } catch (Exception e) {
          LOG.warning("Caught SQLException while testing AuthZ query:\n"
              + e.toString());
          message = res.getString(INVALID_AUTH_QUERY);
          problemFields.add(DBConnectorType.AUTHZ_QUERY);
        }

        // Close database connection and statement.
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
        problemFields.add(DBConnectorType.AUTHZ_QUERY);
      }

      return flag;
    }

    /**
     * Validate the metadata property names.
     *
     * @return true if external metadata related columns are there SQL crawl
     *         query.
     */
    private boolean validateExternalMetadataFields() {
      boolean result = true;

      // Validate Document URL field.
      String documentURLField = config.get(DBConnectorType.DOCUMENT_URL_FIELD);
      if (documentURLField != null && documentURLField.trim().length() > 0) {
        if (!columnNames.contains(documentURLField.trim())) {
          result = false;
          message = res.getString(INVALID_COLUMN_NAME);
          problemFields.add(DBConnectorType.DOCUMENT_URL_FIELD);
        }
      }

      // Validate DocID and Base URL fields.
      String documentIdField = config.get(DBConnectorType.DOCUMENT_ID_FIELD);
      String baseURL = config.get(DBConnectorType.BASE_URL);

      // Check if Base URL field exists without DocId Field.
      if ((baseURL != null && baseURL.trim().length() > 0)
          && (documentIdField == null || documentIdField.trim().length() == 0)) {
        result = false;
        message = res.getString(MISSING_ATTRIBUTES) + ": "
            + res.getString(DBConnectorType.DOCUMENT_ID_FIELD);
        problemFields.add(DBConnectorType.DOCUMENT_ID_FIELD);
      }

      // Validate document ID column name.
      if (documentIdField != null && documentIdField.trim().length() > 0) {
        if (!columnNames.contains(documentIdField)) {
          result = false;
          message = res.getString(INVALID_COLUMN_NAME);
          problemFields.add(DBConnectorType.DOCUMENT_ID_FIELD);
        }
        if (baseURL == null || baseURL.trim().length() == 0) {
          result = false;
          message = res.getString(MISSING_ATTRIBUTES) + ": "
              + res.getString(DBConnectorType.BASE_URL);
          problemFields.add(DBConnectorType.BASE_URL);
        }
      }

      // Validate BLOB/CLOB and Fetch URL field.
      String blobClobField = config.get(DBConnectorType.CLOB_BLOB_FIELD);
      String fetchURL = config.get(DBConnectorType.FETCH_URL_FIELD);

      // Check if Fetch URL field exists without BLOB/CLOB Field.
      if ((fetchURL != null && fetchURL.trim().length() > 0)
          && (blobClobField == null || blobClobField.trim().length() == 0)) {
        result = false;
        message = res.getString(MISSING_ATTRIBUTES) + ": "
            + res.getString(DBConnectorType.CLOB_BLOB_FIELD);
        problemFields.add(DBConnectorType.CLOB_BLOB_FIELD);
      }

      // Check for valid BLOB/CLOB column name.
      if (blobClobField != null && blobClobField.trim().length() > 0) {
        if (!columnNames.contains(blobClobField)) {
          result = false;
          message = res.getString(INVALID_COLUMN_NAME);
          problemFields.add(DBConnectorType.CLOB_BLOB_FIELD);
        }

        if (fetchURL != null && fetchURL.trim().length() > 0
            && !columnNames.contains(fetchURL)) {
          result = false;
          message = res.getString(INVALID_COLUMN_NAME);
          problemFields.add(DBConnectorType.FETCH_URL_FIELD);
        }
      }

      return result;
    }

    /**
     * Validates the name of last modified date column.
     *
     * @return true if result set contains the last modified date column entered
     *         by connector admin, false otherwise.
     */
    private boolean validateLastModifiedField() {
      boolean result = true;
      String lastModifiedDateField =
          config.get(DBConnectorType.LAST_MODIFIED_DATE_FIELD);
      if (!columnNames.contains(lastModifiedDateField)) {
        result = false;
        message = res.getString(INVALID_COLUMN_NAME);
        problemFields.add(DBConnectorType.LAST_MODIFIED_DATE_FIELD);
      }
      return result;
    }

    /**
     * Centralizes the calls to different configuration parameter
     * validation methods.
     *
     * @return true if every validation method return true else return false.
     */
    public boolean validate() {
      password = config.get(DBConnectorType.PASSWORD);
      login = config.get(DBConnectorType.LOGIN);
      connectionUrl = config.get(DBConnectorType.CONNECTION_URL);
      driverClassName = config.get(DBConnectorType.DRIVER_CLASS_NAME);

      // Test JDBC driver class.
      success = testDriverClass();
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
      String lastModDateColumn =
          config.get(DBConnectorType.LAST_MODIFIED_DATE_FIELD);
      if (lastModDateColumn != null && lastModDateColumn.trim().length() > 0) {
        success = validateLastModifiedField();
        if (!success) {
          return success;
        }
      }

      String authZQuery = config.get(DBConnectorType.AUTHZ_QUERY);
      // Validate authZ query if connector admin has provided one.
      if (authZQuery != null && authZQuery.trim().length() > 0) {
        success = validateAuthZQuery(authZQuery);
      }

      // Close database connection, result set and statement.
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
        LOG.log(Level.WARNING, "Caught SQLException closing connection", e);
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
      for (String configKey : DBConnectorType.configKeys) {
        if (!config.containsKey(configKey)
            && !configKey.equalsIgnoreCase(DBConnectorType.EXT_METADATA)) {
          missingAttributes.add(configKey);
        }
      }
      if (missingAttributes.isEmpty()) {
        success = true;
      } else {
        StringBuilder buf = new StringBuilder();
        buf.append(res.getString(MISSING_ATTRIBUTES) + ": ");
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
      for (String field : DBConnectorType.requiredFields) {
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
      xslt = new StringBuilder(config.get(DBConnectorType.XSLT));
      if (xslt != null) {
        String XSLT_RECORD_TITLE_ELEMENT2;
        int index2;
        XSLT_RECORD_TITLE_ELEMENT2 = "<td><xsl:value-of select=\"title\"/>";
        index2 = xslt.indexOf(XSLT_RECORD_TITLE_ELEMENT2);

        if (index2 != -1) {
          success = false;
          message = res.getString("XSLT_VALIDATE");
          problemFields.add(DBConnectorType.XSLT);
        } else {
          success = true;
        }
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
    Map<String, String> config;
    ResourceBundle res;
    private String message = "";
    private boolean success = false;
    private List<String> problemFields = new ArrayList<String>();
    private static final String KEY_VALUE_PLACEHOLDER = "#{value}";
    String[] primaryKeys;
    String sqlCrawlQuery;

    public QueryParameterAndPrimaryKeyCheck(Map<String, String> config,
        ResourceBundle res) {
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
      primaryKeys = config.get(DBConnectorType.PRIMARY_KEYS_STRING).split(PRIMARY_KEYS_SEPARATOR);
      sqlCrawlQuery = config.get(DBConnectorType.SQL_QUERY);
      // This check is required when configuring connector using parameterized
      // crawl query to assure that single primary key is used .
      if (primaryKeys.length > 1
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

  /**
   * Validation Class to check whether HostName is valid.
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
      hostName = config.get(DBConnectorType.HOSTNAME);
      if (hostName.contains(".")) {
        success = true;
      } else {
        message = res.getString(FQDN_HOSTNAME);
        problemFields.add(DBConnectorType.HOSTNAME);
      }
      return success;
    }
  }

  public ConfigValidation validate(Map<String, String> config,
      ResourceBundle resource) {
    boolean success = false;

    ConfigValidation configValidation = new MissingAttributes(config, resource);
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
              configValidation = new QueryParameterAndPrimaryKeyCheck(config,
                  resource);
              success = configValidation.validate();
              if (success) {
                return configValidation;
              }
            }
          }
        }
      }
    }
    return configValidation;
  }
}
