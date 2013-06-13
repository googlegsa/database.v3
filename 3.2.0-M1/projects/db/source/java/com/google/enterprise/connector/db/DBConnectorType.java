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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.spi.ConfigureResponse;
import com.google.enterprise.connector.spi.ConnectorFactory;
import com.google.enterprise.connector.spi.ConnectorType;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.XmlUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link ConnectorType} for {@link DBConnectorType}.
 */
public class DBConnectorType implements ConnectorType {
  private static final Logger LOG =
      Logger.getLogger(DBConnectorType.class.getName());
  private static final String VALUE = "value";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String TEXT = "text";
  private static final String DIV = "div";
  // Red asterisk for required fields.
  public static final String RED_ASTERISK = "*";

  private static final String TYPE = "type";
  private static final String INPUT = "input";
  private static final String TEXT_AREA = "textarea";
  private static final String ROWS = "rows";
  private static final String COLS = "cols";
  private static final String ROWS_VALUE = "10";
  private static final String COLS_VALUE = "50";
  // SIZE is a constant for size attribute of html input field
  private static final String SIZE = "size";
  // SIZE_VALUE is a constant value for size attribute of html input field
  private static final String SIZE_VALUE = "50";

  private static final String CLOSE_ELEMENT_SLASH = "/>";
  private static final String OPEN_ELEMENT = "<";
  private static final String OPEN_ELEMENT_SLASH = "</";
  private static final String CLOSE_ELEMENT = ">";
  private static final String BOLD_TEXT_START = "<b>";
  private static final String BOLD_TEXT_END = "</b>";

  private static final String RADIO = "radio";
  private static final String ALIGN = "align";
  private static final String CENTER = "center";
  private static final String TD_OPEN = "<td";
  private static final String TR_OPEN = "<tr";
  private static final String TR_END = "</tr>\n";
  private static final String TD_END = "</td>\n";
  private static final String CHECKED = "checked";
  private static final String DISABLED = "readonly";
  private static final String HIDDEN = "hidden";
  private static final String TRUE = "true";
  private static final String ON_CLICK = "onClick";

  // These are public for ValidateUtil.
  public static final String COMPLETE_URL = "url";
  public static final String DOC_ID = "docId";
  public static final String BLOB_CLOB = "lob";

  public static final String HOSTNAME = "hostname";
  public static final String CONNECTION_URL = "connectionUrl";
  public static final String LOGIN = "login";
  // Note: "password" is both the name of the property and the form input type.
  public static final String PASSWORD = "password";
  public static final String DRIVER_CLASS_NAME = "driverClassName";
  public static final String DB_NAME = "dbName";
  public static final String SQL_QUERY = "sqlQuery";
  public static final String PRIMARY_KEYS_STRING = "primaryKeysString";
  public static final String XSLT = "xslt";
  public static final String LAST_MODIFIED_DATE_FIELD = "lastModifiedDate";

  // AuthZ Query
  public static final String AUTHZ_QUERY = "authZQuery";
  public static final String DOCUMENT_URL_FIELD = "documentURLField";
  public static final String DOCUMENT_ID_FIELD = "documentIdField";
  public static final String BASE_URL = "baseURL";
  public static final String CLOB_BLOB_FIELD = "lobField";
  public static final String FETCH_URL_FIELD = "fetchURLField";
  public static final String EXT_METADATA = "externalMetadata";
  public static final String NO_EXT_METADATA = "noExt";
  private static final String EXT_METADATA_TYPE = "extMetadataType";

  private static final String COMPLETE_URL_SCRIPT =
      "'javascript:setReadOnlyProperties(false , true , true)'";
  private static final String DOC_ID_SCRIPT =
      "'javascript:setReadOnlyProperties(true , false , true)'";
  private static final String BLOB_CLOB_SCRIPT =
      "'javascript:setReadOnlyProperties(true , true , false)'";

  /** List of required fields. */
  public static List<String> requiredFields = Arrays.asList(new String[] { HOSTNAME,
      CONNECTION_URL, DB_NAME, LOGIN, DRIVER_CLASS_NAME, SQL_QUERY,
      PRIMARY_KEYS_STRING });

  public static final List<String> configKeys =
      ImmutableList.<String>builder().add(
          // JDBC connection
          DRIVER_CLASS_NAME,
          CONNECTION_URL,
          LOGIN,
          PASSWORD,
          // SQL query and column names
          SQL_QUERY,
          PRIMARY_KEYS_STRING,
          LAST_MODIFIED_DATE_FIELD,
          // Indexing
          HOSTNAME,
          DB_NAME,
          XSLT,
          // AuthZ
          AUTHZ_QUERY,
          // More indexing
          EXT_METADATA,
          DOCUMENT_URL_FIELD,
          DOCUMENT_ID_FIELD,
          BASE_URL,
          CLOB_BLOB_FIELD,
          FETCH_URL_FIELD,
          EXT_METADATA_TYPE
        ).build();

  public DBConnectorType() {
  }

  /**
   * Gets the initial/blank config form.
   *
   * @param locale
   * @return result ConfigureResponse which contains the form snippet.
   */
  /* @Override */
  public ConfigureResponse getConfigForm(Locale locale) {
    return getPopulatedConfigForm(ImmutableMap.<String, String>of(), locale);
  }

  /* @Override */
  public ConfigureResponse getPopulatedConfigForm(
      Map<String, String> configMap, Locale locale) {
    ConfigForm configForm;
    try {
      configForm = new ConfigForm(configMap, locale);
    } catch (MissingResourceException e) {
      LOG.log(Level.WARNING, "Failed to load language resource.", e);
      return new ConfigureResponse("Failed to load language resource: "
                                   + e.getMessage(), "");
    }
    return new ConfigureResponse("", configForm.getFormSnippet());
  }

  /**
   * Make sure the configuration map contains all the necessary attributes and
   * that we can instantiate a connector using the provided configuration.
   */
  /* @Override */
  public ConfigureResponse validateConfig(Map<String, String> configMap,
      Locale locale, ConnectorFactory factory) {
    ConfigForm configForm;
    try {
      configForm = new ConfigForm(configMap, locale);
    } catch (MissingResourceException e) {
      LOG.log(Level.WARNING, "Failed to load language resource.", e);
      return new ConfigureResponse("Failed to load language resource: "
                                   + e.getMessage(), "");
    }
    ConfigValidation configValidation = configForm.validateConfig();
    if (configValidation.validate()) {
      try {
        factory.makeConnector(configMap);
      } catch (RepositoryException e) {
        LOG.log(Level.WARNING, "failed to create connector", e);
        return new ConfigureResponse("Error creating connector: "
            + e.getMessage(), "");
      }
      return null;
    }
    return new ConfigureResponse(configValidation.getMessage(),
        configForm.getValidatedFormSnippet(configValidation));
  }

  private static class ConfigForm {
    private static final String LOCALE_DB = "config/DbConnectorResources";
    private final ResourceBundle resource;
    private final Map<String, String> configMap;
    private boolean isDocIdDisabled = false;
    private boolean isLOBFieldDisable = false;

    public ConfigForm(Map<String, String> configMap, Locale locale) 
        throws MissingResourceException {
      this.resource = ResourceBundle.getBundle(LOCALE_DB, locale);
      this.configMap = configMap;
    }

    /**
     * Makes a config form snippet using the supplied keys and, if we have a
     * non-null config map, pre-filling values in from that map.
     *
     * @return config form snippet
     */
    public String getFormSnippet() {
      StringBuilder buf = new StringBuilder(getJavaScript());
      for (String key : configKeys) {
        buf.append(formSnippetWithColor(key, configMap.get(key), false));
      }
      return buf.toString();
    }

    /**
     * Makes a database connector configuration form snippet using supplied key
     * and value.
     *
     * @param key for html form field of configuration form
     * @param value of html form field of configuration form
     * @param red indicates whether this field is required or not
     * @return database connector configuration form snippet
     */
    private String formSnippetWithColor(String key, String value, boolean red) {
      StringBuilder buf = new StringBuilder();
      appendStartRow(buf, key, red, value);

      // Check if key is "externalMetadata". For this label we don't have to
      // create corresponding Text Field/Area . End TD and TR elements and return.
      if (EXT_METADATA.equalsIgnoreCase(key)) {
        appendEndRow(buf);
        return buf.toString();
      }

      buf.append(OPEN_ELEMENT);

      // Create text area for SQL Query, XSLT and AuthZ Query fields.
      if (key.equals(SQL_QUERY) || key.equals(XSLT) || AUTHZ_QUERY.equals(key)) {
        buf.append(TEXT_AREA);
        appendAttribute(buf, ROWS, ROWS_VALUE);
        appendAttribute(buf, COLS, COLS_VALUE);
        appendAttribute(buf, NAME, key);
        appendAttribute(buf, ID, key);
        buf.append(CLOSE_ELEMENT);
        if (null != value) {
          buf.append("<![CDATA[" + value + "]]>");
        }
        buf.append(OPEN_ELEMENT_SLASH);
        buf.append(TEXT_AREA);
        buf.append(CLOSE_ELEMENT);
      } else if (EXT_METADATA_TYPE.equals(key)) {
        buf.append(INPUT);
        appendAttribute(buf, NAME, EXT_METADATA_TYPE);
        appendAttribute(buf, TYPE, HIDDEN);
        appendAttribute(buf, ID, key);
        appendAttribute(buf, VALUE, NO_EXT_METADATA);
        buf.append(CLOSE_ELEMENT_SLASH);
      } else {
        buf.append(INPUT);
        if (isKeySensitive(key)) {
          appendAttribute(buf, TYPE, PASSWORD);
        } else {
          appendAttribute(buf, TYPE, TEXT);
        }
        appendAttribute(buf, SIZE, SIZE_VALUE);
        appendAttribute(buf, NAME, key);
        appendAttribute(buf, ID, key);

        if (null != value) {
          appendAttribute(buf, VALUE, value);
        }

        setReadOnly(key, value, buf);

        buf.append(CLOSE_ELEMENT_SLASH);
      }

      appendEndRow(buf);
      return buf.toString();
    }

    /** Returns true, if a key's value should be masked. */
    private boolean isKeySensitive(String key) {
      return key.toLowerCase().contains(PASSWORD);
    }

    public ConfigValidation validateConfig() {
      return new ValidateUtil().validate(configMap, resource);
    }

    /**
     * Makes a validated config form snippet using the supplied config Map.
     * If the values are correct, they get pre-filled otherwise, the key in
     * the form is red.
     *
     * @param configValidation
     * @return validated config form snippet
     */
    public String getValidatedFormSnippet(ConfigValidation configValidation) {
      StringBuilder buf = new StringBuilder();
      buf.append(getJavaScript());
      List<String> problemfields = configValidation.getProblemFields();
      for (String key : configKeys) {
        buf.append(formSnippetWithColor(key, configMap.get(key),
                                        problemfields.contains(key)));
      }
      return buf.toString();
    }

    /**
     * Creates the 'TR' and 'TD' elements for Fields Labels. Field
     * labels are displayed in RED if there is any validation error.
     *
     * @param buf
     * @param key
     * @param red
     * @param value
     */
    private void appendStartRow(StringBuilder buf, String key, boolean red,
        String value) {

      buf.append(TR_OPEN);
      appendAttribute(buf, "valign", "top");
      buf.append(CLOSE_ELEMENT);

      if (BASE_URL.equalsIgnoreCase(key) || FETCH_URL_FIELD.equalsIgnoreCase(key)) {
        buf.append(TD_OPEN + " " + ALIGN + "='" + CENTER + "'" + CLOSE_ELEMENT);
      } else {
        buf.append(TD_OPEN);
        appendAttribute(buf, "colspan", "1");
        appendAttribute(buf, "rowspan", "1");
        appendAttribute(buf, "style", "white-space:nowrap");
        buf.append(CLOSE_ELEMENT);

        if (EXT_METADATA.equalsIgnoreCase(key)) {
          buf.append(BOLD_TEXT_START);
        }
      }

      if (red) {
        buf.append("<font color=\"red\">");
      }

      // Add radio buttons before "Stylesheet", "Document URL Field",
      // "Document Id Field" and "BLOB/CLOB Field"
      if (DOCUMENT_URL_FIELD.equals(key)) {
        // Set isChecked flag true only if value of Document URL Field is not
        // empty.
        boolean isChecked = value != null && value.trim().length() > 0;
        appendRadio(buf, COMPLETE_URL, isChecked);
      } else if (DOCUMENT_ID_FIELD.equals(key)) {
        String baseURL = configMap.get(BASE_URL);
        // Set isChecked flag true if value of Document ID field is not empty or
        // if user has entered value for base URL.
        boolean isChecked = (value != null && value.trim().length() > 0)
            || (baseURL != null && baseURL.trim().length() > 0);
        appendRadio(buf, DOC_ID, isChecked);
      } else if (CLOB_BLOB_FIELD.equals(key)) {
        String fetchURL = configMap.get(FETCH_URL_FIELD);
        boolean isChecked = (value != null && value.trim().length() > 0)
            || (fetchURL != null && fetchURL.trim().length() > 0);
        appendRadio(buf, BLOB_CLOB, isChecked);
      }

      // No label for External Metadata Type (Radio button).
      if (!EXT_METADATA_TYPE.equalsIgnoreCase(key)) {
        buf.append(OPEN_ELEMENT);
        buf.append(DIV);
        appendAttribute(buf, "style", "float: left;");
        buf.append(CLOSE_ELEMENT);
        buf.append(resource.getString(key));
        buf.append(OPEN_ELEMENT_SLASH);
        buf.append(DIV);
        buf.append(CLOSE_ELEMENT);
      }

      if (red) {
        buf.append("</font>");
      }
      if (EXT_METADATA.equalsIgnoreCase(key)) {
        buf.append(BOLD_TEXT_END);
      }

      // Add red asterisk for required fields.
      if (requiredFields.contains(key)) {
        buf.append(OPEN_ELEMENT);
        buf.append(DIV);
        appendAttribute(buf, "style", "text-align: right; color: red; "
                        + "font-weight: bold; margin-right: 0.3em;");
        buf.append(CLOSE_ELEMENT);
        buf.append(RED_ASTERISK);
        buf.append(OPEN_ELEMENT_SLASH);
        buf.append(DIV);
        buf.append(CLOSE_ELEMENT);
      }

      buf.append(TD_END);
      buf.append(TD_OPEN);
      appendAttribute(buf, "colspan", "1");
      appendAttribute(buf, "rowspan", "1");
      appendAttribute(buf, "style", "white-space:nowrap");
      buf.append(CLOSE_ELEMENT);
    }

    private void appendEndRow(StringBuilder buf) {
      buf.append(TD_END);
      buf.append(TR_END);
    }

    private void appendAttribute(StringBuilder buf, String attrName,
                                 String attrValue) {
      try {
        XmlUtils.xmlAppendAttr(attrName, attrValue, buf);
      } catch (IOException e) {
        // Can't happen with StringBuilder.
        throw new AssertionError(e);
      }
    }

    private void appendRadio(StringBuilder buf, String value, boolean isChecked)
    {
      buf.append(OPEN_ELEMENT + INPUT + " " + TYPE + "=" + "'" + RADIO
          + "' " + NAME + "=" + "'" + EXT_METADATA_TYPE + "' " + VALUE + "=" 
          + "'" + value + "' ");
      if (isChecked) {
        buf.append(CHECKED + "=" + "'" + CHECKED + "' ");
      }
      if (COMPLETE_URL.equals(value)) {
        buf.append(ON_CLICK + "=" + COMPLETE_URL_SCRIPT);
      } else if (DOC_ID.equals(value)) {
        buf.append(ON_CLICK + "=" + DOC_ID_SCRIPT);
      } else if (BLOB_CLOB.equals(value)) {
        buf.append(ON_CLICK + "=" + BLOB_CLOB_SCRIPT);
      }
      buf.append(CLOSE_ELEMENT_SLASH);
    }

    /**
     * Builds the JavaScript for making External metadata related
     * fields (Document URL Field , Document Id Field , Base URL , BLOB/CLOB Field
     * and Fetch URL Field) and "AuthZ Query" field editable/non-editable
     * depending upon user selection. When user selects any of the external
     * metadata radio button other fields becomes non-editable and previous value
     * will be cleared. When user selects "Document URL Field" OR
     * "Document Id Field" "AuthZ Query" will become non-editable.
     *
     * @return JavaScript for making External Metadta fields and authZ query field
     *         editable/non-editable depending upon context.
     */
    private static String getJavaScript() {
      /*
       * urlField , docIdField , lobField are boolean values for making external
       * metadata fields readOnly. "AuthZ Field" will become editable when user
       * selects BLOB/CLOB Field i.e when BLOB/CLOB field is editable.
       */
      String javascript = "<SCRIPT> "
          + "function setReadOnlyProperties(urlField , docIdField , lobField){"
          + "document.getElementById('documentURLField').readOnly=urlField ;    "
          + "document.getElementById('documentIdField').readOnly=docIdField ;    "
          + "document.getElementById('baseURL').readOnly=docIdField ;    "
          + "document.getElementById('lobField').readOnly=lobField ;  "
          + "document.getElementById('fetchURLField').readOnly=lobField ;"
          + "if(urlField){document.getElementById('documentURLField').value='';}"
          + "if(docIdField){document.getElementById('documentIdField').value='' ;"
          + "document.getElementById('baseURL').value=''}"
          + "if(lobField){document.getElementById('lobField').value='';"
          + "document.getElementById('fetchURLField').value='';}"
          + "if(!lobField){document.getElementById('authZQuery').readOnly=false}"
          + "else{document.getElementById('authZQuery').readOnly=true} }"
          + "</SCRIPT>";

      return javascript;
    }

    /**
     * Set readOnly='true' for External Metadata fields like
     * "Document URl Field", "Document Id Field" and "Base URL Field". "Base URL"
     * and "Fetch URL" fields are set read-only , only when "Document ID Field"
     * and "BLOB/CLOB Field" are read only respectively.
     *
     * @param key
     * @param value
     * @param buf
     */
    private void setReadOnly(String key, String value, StringBuilder buf) {
      // Set fields non-editable only if they are empty.
      if (value == null || value.trim().equals("")) {
        if (DOCUMENT_URL_FIELD.equals(key)) {
          // Set "Document URL Field" non-editable.
          appendAttribute(buf, DISABLED, TRUE);
        } else if (DOCUMENT_ID_FIELD.equals(key)) {
          // Set "Document Id Field" non-editable only if user has not entered
          // value for base URL.
          String baseURL = configMap.get(BASE_URL);
          if (baseURL == null || baseURL.trim().length() == 0) {
            appendAttribute(buf, DISABLED, TRUE);
            isDocIdDisabled = true;
          }
        } else if (BASE_URL.equals(key) && isDocIdDisabled) {
          // Set "Base URL" field non-editable if "Document Id Field" field is
          // non-editable.
          appendAttribute(buf, DISABLED, TRUE);
          isDocIdDisabled = false;
        } else if (CLOB_BLOB_FIELD.equals(key)) {
          // Set "BLOB/CLOB Field" non-editable only if user has not entered value
          // for fetch URL.
          String fetchURL = configMap.get(FETCH_URL_FIELD);
          if (fetchURL == null || fetchURL.trim().length() == 0) {
            appendAttribute(buf, DISABLED, TRUE);
            isLOBFieldDisable = true;
          }
        } else if (FETCH_URL_FIELD.equals(key) && isLOBFieldDisable) {
          // Set "Fetch URL" field not editable if "BLOB/CLOB Field" field is
          // non-editable.
          appendAttribute(buf, DISABLED, TRUE);
          isLOBFieldDisable = false;
        }
      }
    }
  }
}
