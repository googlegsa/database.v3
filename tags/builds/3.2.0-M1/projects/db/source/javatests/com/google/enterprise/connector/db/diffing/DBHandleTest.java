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

import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.spiimpl.DateValue;
import com.google.enterprise.connector.util.diffing.DocumentHandle;

import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class DBHandleTest extends TestCase {

  private String lastModified;
  private Map<String, String> properties;
  private JsonDocument jsonDocument;

  protected void setUp() throws Exception {
    long lastModifiedMillis = new Date().getTime();
    Calendar lastModifiedCalendar = Calendar.getInstance();
    lastModifiedCalendar.setTimeInMillis(lastModifiedMillis);
    lastModified = Value.calendarToIso8601(lastModifiedCalendar);

    properties = ImmutableMap.of(
        SpiConstants.PROPNAME_DOCID, "1",
        SpiConstants.PROPNAME_ISPUBLIC, "false",
        SpiConstants.PROPNAME_MIMETYPE, "text/plain",
        SpiConstants.PROPNAME_LASTMODIFIED, lastModified);

    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      jsonObjectUtil.setProperty(entry.getKey(), entry.getValue());
    }
    // Overwrites the string in jsonObjectUtil with a date value.
    jsonObjectUtil.setLastModifiedDate(SpiConstants.PROPNAME_LASTMODIFIED,
        new Timestamp(lastModifiedMillis));
    jsonDocument = new JsonDocument(jsonObjectUtil.getProperties(),
                                    jsonObjectUtil.getJsonObject());
  }

  public void testGetDocument() throws Exception {
    DocumentHandle handle = new DBHandle(jsonDocument);
    Document doc = handle.getDocument();
    assertNotNull(doc);
    assertEquals(properties.keySet(), doc.getPropertyNames());

    for (Map.Entry<String, String>  entry : properties.entrySet()) {
      Property property = doc.findProperty(entry.getKey());
      assertNotNull(property);
      assertEquals(entry.getValue(), property.nextValue().toString());
    }
  }

  public void testGetDocumentId() {
    DBHandle handle = new DBHandle(jsonDocument);
    String expected = "1";
    assertEquals(expected, handle.getDocumentId());
  }

  /**
   * Tests that the JSON object handle string includes all of the
   * document properties.
   */
  public void testToString() {
    DocumentHandle handle = new DBHandle(jsonDocument);
    Object expected = "{\"google:ispublic\":\"false\",\"google:docid\":\"1\","
        + "\"google:mimetype\":\"text/plain\","
        + "\"google:lastmodified\":\"" + lastModified + "\"}";
    assertEquals(expected, handle.toString());
  }

  /**
   * Tests that google:lastmodified is deserialized as a DateValue.
   * This test could go in DBSnapshotRepositoryTest, where the main
   * lifecycle tests are, or in JsonDocumentTest, since the date
   * handling is in JsonDocument at the moment, but we're
   * fundamentally testing the behavior of the document handle.
   */
  public void testDeserializedDate() throws RepositoryException {
    DocumentHandle handle = new DBHandle(jsonDocument);
    Document document = handle.getDocument();
    DocumentHandle deserialHandle = new DBHandle(handle.toString());
    Document deserialDocument = deserialHandle.getDocument();

    Value value = Value.getSingleValue(document,
        SpiConstants.PROPNAME_LASTMODIFIED);
    Value deserialValue = Value.getSingleValue(deserialDocument,
        SpiConstants.PROPNAME_LASTMODIFIED);
    assertEquals(lastModified, value.toString());
    assertEquals(value.toString(), deserialValue.toString());
    assertTrue(value.getClass().toString(), value instanceof DateValue);
    assertEquals(value.getClass(), deserialValue.getClass());
  }
}
