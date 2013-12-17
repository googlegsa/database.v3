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

package com.google.enterprise.connector.db.diffing;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DBTestBase;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.spiimpl.BinaryValue;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class DocumentBuilderFixture extends DBTestBase {
  private static final Logger LOG =
      Logger.getLogger(DocumentBuilderFixture.class.getName());

  protected final ProductionTraversalContext context =
      new ProductionTraversalContext();
  protected final String primaryKeyColumn = "id";

  protected DBContext dbContext;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    LOG.info("Test " + getName());

    // This is usually done by RepositoryHandler.
    JsonDocument.setTraversalContext(context);

    dbContext = super.getDbContext();
    dbContext.setPrimaryKeys(primaryKeyColumn);
    dbContext.setGoogleConnectorName("test_connector");
  }

  public static String getProperty(JsonDocument doc, String propName)
      throws IOException, RepositoryException {
    Value value = Value.getSingleValue(doc, propName);
    if (value == null) {
      return null;
    } else if (propName.equals(SpiConstants.PROPNAME_CONTENT)) {
      byte[] content = getBytes(value);
      return new String(content, Charsets.UTF_8);
    } else {
      return value.toString();
    }
  }

  protected static byte[] readBlobContent(JsonDocument doc)
      throws IOException, RepositoryException {
    Value value = Value.getSingleValue(doc, SpiConstants.PROPNAME_CONTENT);
    if (value == null) {
      return null;
    }
    return getBytes(value);
  }

  private static byte[] getBytes(Value value)
      throws IOException, RepositoryException {
    InputStream is = ((BinaryValue) value).getInputStream();
    byte[] blobContent = ByteStreams.toByteArray(is);
    is.close();
    return blobContent;
  }

  /** Uses a builder to create a new document from the given database row. */
  public static JsonDocument getJsonDocument(DocumentBuilder builder,
      Map<String, Object> row) throws DBException, RepositoryException {
    return (JsonDocument) builder.getDocumentSnapshot(row).getUpdate(null)
        .getDocument();
  }

  /** Provides getter and setter methods for a DBContext property. */
  protected static class FieldNameBean {
    protected final DBContext dbContext;
    private final Method getter;
    private final Method setter;

    /**
     * Constructor for subclasses that provide their own get and set
     * implementation.
     */
    protected FieldNameBean(DBContext dbContext) {
      this.dbContext = dbContext;
      getter = null;
      setter = null;
    }

    public FieldNameBean(DBContext dbContext, String propertyName) {
      this.dbContext = dbContext;
      try {
        PropertyDescriptor descriptor =
            new PropertyDescriptor(propertyName, DBContext.class);
        getter = descriptor.getReadMethod();
        setter = descriptor.getWriteMethod();
        Assert.assertNotNull("Unable to find setter method for " + propertyName,
            setter);
      } catch (IntrospectionException e) {
        throw new RuntimeException(e);
      }
    }

    public String get() {
      try {
        return (String) getter.invoke(dbContext);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void set(String value) {
      try {
        setter.invoke(dbContext, value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public List<String> getNameVariations() {
      String name = get();
      if (Util.isNullOrWhitespace(name)) {
        // ImmutableList does not support nulls.
        return Arrays.asList(null, "", " ");
      } else {
        return Arrays.asList(name, " " + name, name + " ");
      }
    }
  }

  /**
   * Tests different cases and extra whitespace in field names. All of
   * the errors are collected before reporting the failures. Bean
   * introspection on the {@code configName} is used to get and set
   * the field name values.
   *
   * @param configName the DBContext bean property name
   * @param builder the builder instance under test
   * @param row the database row
   * @param propertyName the affected document property name
   * @param expectedValue the expected value of the document property
   */
  protected void testFieldName(String configName,
      DocumentBuilder builder, Map<String, Object> row, String propertyName,
      Object expectedValue) {
    FieldNameBean bean = new FieldNameBean(dbContext, configName);
    testFieldName(configName, bean, builder, row, propertyName, expectedValue);
  }

  /**
   * Tests different cases and extra whitespace in field names. All of
   * the errors are collected before reporting the failures.
   *
   * @param configName the DBContext bean property name
   * @param bean the bean used to get and set the bean property
   * @param builder the builder instance under test
   * @param row the database row
   * @param propertyName the affected document property name
   * @param expectedValue the expected value of the document property
   */
  protected void testFieldName(String configName, FieldNameBean bean,
      DocumentBuilder builder, Map<String, Object> row, String propertyName,
      Object expectedValue) {
    String originalName = bean.get();

    StringBuilder errors = new StringBuilder();
    for (String fieldName : bean.getNameVariations()) {
      bean.set(fieldName);
      try {
        JsonDocument doc = getJsonDocument(builder, row);
        String propertyValue = getProperty(doc, propertyName);
        if (!expectedValue.equals(propertyValue)) {
          errors.append(configName + "=" + fieldName + ": expected:<" +
              expectedValue + "> but was:<" + propertyValue + ">\n");
        }

        if (!Util.isNullOrWhitespace(originalName)) {
          // The original name should be in the skip set.
          String metadataValue = getProperty(doc, originalName);
          if (metadataValue != null) {
            errors.append(configName + "=" + fieldName
                + ": expected skipped field but was:<" + metadataValue + ">\n");
          }
        }
      } catch (Exception e) {
        LOG.log(Level.WARNING, e.getMessage(), e);
        errors.append(configName + "=" + fieldName + ": " + e + "\n");
      }
    }

    if (errors.length() > 0) {
      fail(errors.toString());
    }
  }
}
