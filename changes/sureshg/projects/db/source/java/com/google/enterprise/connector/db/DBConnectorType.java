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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.enterprise.connector.spi.ConfigureResponse;
import com.google.enterprise.connector.spi.ConnectorFactory;
import com.google.enterprise.connector.spi.ConnectorType;
import com.google.enterprise.connector.spi.RepositoryException;
import com.ibatis.common.jdbc.SimpleDataSource;

/**
 * Implementation of {@link ConnectorType} for {@link DBConnector}.
 */
public class DBConnectorType implements ConnectorType {
	private static final Logger LOG = Logger.getLogger(DBConnectorType.class.getName());
	private static final String LOCALE_DB = "config/DbConnectorResources";
	private ResourceBundle resource;
	private static final String TEST_CONNECTIVITY = "TEST_CONNECTIVITY";
	private static final String TEST_DRIVER_CLASS = "TEST_DRIVER_CLASS";
	private static final String TEST_SQL_QUERY = "TEST_SQL_QUERY";
	private static final String TEST_PRIMARY_KEYS = "TEST_PRIMARY_KEYS";
	private static final String FQDN_HOSTNAME = "FQDN_HOSTNAME";
	private static final String MISSING_ATTRIBUTES = "MISSING_ATTRIBUTES";
	private static final String REQ_FIELDS = "REQ_FIELDS";
	private static final String VALUE = "value";
	private static final String NAME = "name";
	private static final String TEXT = "text";
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
	private static final String SIZE_VALUE = "40";

	private static final String CLOSE_ELEMENT_SLASH = "/>";
	private static final String OPEN_ELEMENT = "<";
	private static final String OPEN_ELEMENT_SLASH = "</";
	private static final String CLOSE_ELEMENT = ">";
	private static final String PASSWORD = "password";
	private static final String TR_END = "</tr>\n";
	private static final String TD_END = "</td>\n";
	private static final String TD_START = "<td>";
	private static final String TR_START = "<tr>\n";

	private static final String HOSTNAME = "hostname";
	private static final String CONNECTION_URL = "connectionUrl";
	private static final String LOGIN = "login";
	private static final String DRIVER_CLASS_NAME = "driverClassName";
	private static final String DB_NAME = "dbName";
	private static final String SQL_QUERY = "sqlQuery";
	private static final String PRIMARY_KEYS_STRING = "primaryKeysString";
	private static final String XSLT = "xslt";

	private final Set<String> configKeys;
	private String initialConfigForm = null;

	/**
	 * @param configKeys names of required configuration variables.
	 */
	public DBConnectorType(Set<String> configKeys) {
		if (configKeys == null) {
			throw new RuntimeException("configKeys must be non-null");
		}
		this.configKeys = configKeys;
	}

	/**
	 * Gets the initial/blank form.
	 * 
	 * @return HTML form as string
	 */
	private String getInitialConfigForm() {
		if (initialConfigForm != null) {
			return initialConfigForm;
		}
		if (configKeys == null) {
			throw new IllegalStateException();
		}
		this.initialConfigForm = makeConfigForm(null);
		return initialConfigForm;
	}

	/**
	 * Makes a config form snippet using the supplied keys and, if passed a
	 * non-null config map, pre-filling values in from that map.
	 * 
	 * @param configMap
	 * @return config form snippet
	 */
	private String makeConfigForm(Map<String, String> configMap) {
		StringBuilder buf = new StringBuilder();
		for (String key : configKeys) {
			String value;
			if (configMap != null) {
				value = configMap.get(key);
			} else {
				value = null;
			}
			buf.append(formSnippetWithColor(key, value, false));
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
		appendStartRow(buf, key, red);
		buf.append(OPEN_ELEMENT);
		if (key.equals(SQL_QUERY) || key.equals(XSLT)) {
			buf.append(TEXT_AREA);
			appendAttribute(buf, ROWS, ROWS_VALUE);
			appendAttribute(buf, COLS, COLS_VALUE);
			appendAttribute(buf, NAME, key);
			buf.append(CLOSE_ELEMENT);
			if (null != value) {
				buf.append("<![CDATA[" + value + "]]>");
			}
			buf.append(OPEN_ELEMENT_SLASH);
			buf.append(TEXT_AREA);
			buf.append(CLOSE_ELEMENT);
		} else {
			buf.append(INPUT);
			if (key.equalsIgnoreCase(PASSWORD)) {
				appendAttribute(buf, TYPE, PASSWORD);
			} else {
				appendAttribute(buf, TYPE, TEXT);
			}
			appendAttribute(buf, SIZE, SIZE_VALUE);
			appendAttribute(buf, NAME, key);
			if (null != value) {
				appendAttribute(buf, VALUE, value);
			}
			buf.append(CLOSE_ELEMENT_SLASH);
		}
		appendEndRow(buf);
		return buf.toString();
	}

	/**
	 * Makes a validated config form snippet using the supplied config Map. If
	 * the values are correct, they get pre-filled otherwise, the key in the
	 * form is red.
	 * 
	 * @param config
	 * @return validated config form snippet
	 */
	private String makeValidatedForm(Map<String, String> config) {
		StringBuilder buf = new StringBuilder();
		List<String> problemfields;
		boolean success = false;
		ConfigValidation configValidation;
		configValidation = new MissingAttributes(config, resource);
		success = configValidation.validate();
		if (success) {
			configValidation = new RequiredFields(config, resource);
			success = configValidation.validate();
			if (success) {
				configValidation = new TestDbConnectivity(config, resource);
				success = configValidation.validate();
				if (success) {
					configValidation = new HostNameFQDNCheck(config, resource);
					success = configValidation.validate();
				}
			}
		}
		problemfields = configValidation.getProblemFields();
		for (String key : configKeys) {
			String value = config.get(key);
			if (problemfields.contains(key)) {
				buf.append(formSnippetWithColor(key, value, true));
			} else {
				buf.append(formSnippetWithColor(key, value, false));
			}
		}
		return buf.toString();
	}

	private void appendStartRow(StringBuilder buf, String key, boolean red) {
		buf.append(TR_START);
		buf.append(TD_START);
		if (red) {
			buf.append("<font color=\"red\">");
		}
		buf.append(resource.getString(key));
		if (red) {
			buf.append("</font>");
		}
		buf.append(TD_END);
		buf.append(TD_START);
	}

	private void appendEndRow(StringBuilder buf) {
		buf.append(TD_END);
		buf.append(TR_END);
	}

	private void appendAttribute(StringBuilder buf, String attrName,
			String attrValue) {
		buf.append(" ");
		// TODO(meghna): Change this when the xmlAppendAttrValuePair takes
		// StringBuilder or Appendable as an argument.
		StringBuffer strBuf = new StringBuffer();
		com.google.enterprise.connector.spi.XmlUtils.xmlAppendAttrValuePair(attrName, attrValue, strBuf);
		buf.append(strBuf.toString());
	}

	/*
	 * Interface for validation of the config map.
	 */
	private interface ConfigValidation {
		boolean validate();

		String getMessage();

		List<String> getProblemFields();
	}

	/**
	 * Tests the connectivity to the database.
	 */
	private static class TestDbConnectivity implements ConfigValidation {
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

		public TestDbConnectivity(Map<String, String> config, ResourceBundle res) {
			this.config = config;
			this.res = res;
		}

		public boolean validate() {
			Statement stmt = null;
			Connection conn = null;
			ResultSet resultSet = null;
			boolean result = false;
			password = config.get(PASSWORD);
			login = config.get(LOGIN);
			connectionUrl = config.get(CONNECTION_URL);
			driverClassName = config.get(DRIVER_CLASS_NAME);
			if (driverClassName != null && connectionUrl != null
					&& login != null && password != null) {
				Map<String, String> jdbcProps = new TreeMap<String, String>();
				jdbcProps.put(JDBC_CONNECTION_URL_STR, connectionUrl);
				jdbcProps.put(JDBC_DRIVER_STR, driverClassName);
				jdbcProps.put(JDBC_USERNAME_STR, login);
				jdbcProps.put(JDBC_PASSWORD_STR, password);
				SimpleDataSource sds = null;

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

				/*
				 * below if block is for testing connection with the database
				 * with given values of input parameters.
				 */
				if (sds != null) {
					try {
						conn = sds.getConnection();
					} catch (SQLException e) {
						LOG.warning("Caught SQLException while testing connection: "
								+ "\n" + e.toString());
						message = res.getString(TEST_CONNECTIVITY);
						// TODO(meghna): See if there is a way to pin point the
						// actual
						// problematic fields.
						problemFields.add(DRIVER_CLASS_NAME);
						problemFields.add(LOGIN);
						problemFields.add(PASSWORD);
						problemFields.add(CONNECTION_URL);
					}
				}
				/*
				 * Block to test SQL query. SQL query should be of type SELECT,
				 * it should not be DML statement.
				 */
				if (conn != null) {
					try {
						conn.setAutoCommit(false);
						conn.setReadOnly(true);
						stmt = conn.createStatement();
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

				/*
				 * This block of code is to validate primary keys.
				 */
				if (result) {
					try {
						resultSet = stmt.getResultSet();
						if (resultSet != null) {
							ResultSetMetaData rsMeta = resultSet.getMetaData();
							int columnCount = rsMeta.getColumnCount();
							String[] primaryKeys = config.get(PRIMARY_KEYS_STRING).split(",");
							boolean flag = false;
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

				}
			}
			/*
			 * close dabase connection, result set and statement
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
				LOG.info("Caught SQLException " + e.toString());
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
				if (!config.containsKey(configKey)) {
					missingAttributes.add((String) configKey);
				}
			}
			if (missingAttributes.isEmpty()) {
				success = true;
			} else {
				StringBuilder buf = new StringBuilder();
				buf.append(res.getString(MISSING_ATTRIBUTES));
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
				buf.append(res.getString(REQ_FIELDS));
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

	/**
	 * Gets the initial/blank config form.
	 * 
	 * @param locale
	 * @return result ConfigureResponse which contains the form snippet.
	 */
	/* @Override */
	public ConfigureResponse getConfigForm(Locale locale) {
		// TODO(meghna): See if this is thread safe.
		try {
			resource = ResourceBundle.getBundle(LOCALE_DB, locale);
		} catch (MissingResourceException e) {
			resource = ResourceBundle.getBundle(LOCALE_DB);
		}
		ConfigureResponse result = new ConfigureResponse("",
				getInitialConfigForm());
		LOG.info("getConfigForm form:\n" + result.getFormSnippet());
		return result;
	}

	/* @Override */
	public ConfigureResponse getPopulatedConfigForm(
			Map<String, String> configMap, Locale locale) {
		try {
			resource = ResourceBundle.getBundle(LOCALE_DB, locale);
		} catch (MissingResourceException e) {
			resource = ResourceBundle.getBundle(LOCALE_DB);
		}
		ConfigureResponse result = new ConfigureResponse("",
				makeConfigForm(configMap));
		return result;
	}

	/**
	 * Make sure the configuration map contains all the necessary attributes and
	 * that we can instantiate a connector using the provided configuration.
	 */
	/* @Override */
	public ConfigureResponse validateConfig(Map<String, String> config,
			Locale locale, ConnectorFactory factory) {
		boolean success = false;
		try {
			resource = ResourceBundle.getBundle(LOCALE_DB, locale);
		} catch (MissingResourceException e) {
			resource = ResourceBundle.getBundle(LOCALE_DB);
		}
		ConfigValidation configValidation = new MissingAttributes(config,
				resource);
		success = configValidation.validate();
		if (success) {
			configValidation = new RequiredFields(config, resource);
			success = configValidation.validate();
			if (success) {
				configValidation = new TestDbConnectivity(config, resource);
				success = configValidation.validate();
				if (success) {
					configValidation = new HostNameFQDNCheck(config, resource);
					success = configValidation.validate();
					if (success) {
						try {
							factory.makeConnector(config);
						} catch (RepositoryException e) {
							LOG.log(Level.INFO, "failed to create connector", e);
							return new ConfigureResponse(
									"Error creating connector: "
											+ e.getMessage(), "");
						}
						return null;
					}
				}
			}
		}
		String form = makeValidatedForm(config);
		return new ConfigureResponse(configValidation.getMessage(), form);
	}
}
