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
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;

import junit.framework.TestCase;

import java.util.Map;

public class DBHandleTest extends TestCase {

  Map<String, String> properties;
  JsonDocument jsonDocument;

  protected void setUp() throws Exception {
    properties = ImmutableMap.of(
        SpiConstants.PROPNAME_DOCID, "1",
        SpiConstants.PROPNAME_ISPUBLIC, "false",
        SpiConstants.PROPNAME_MIMETYPE, "text/plain");

    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      jsonObjectUtil.setProperty(entry.getKey(), entry.getValue());
    }
    jsonObjectUtil.setProperty(JsonDocumentUtil.ROW_CHECKSUM, "1234");
    jsonDocument = new JsonDocument(jsonObjectUtil.getProperties(),
                                    jsonObjectUtil.getJsonObject());
  }

  public void testGetDocument() throws Exception {
    DocumentHandle handle = new DBHandle(jsonDocument);
    Document doc = handle.getDocument();
    assertNotNull(doc);
    assertEquals(properties.keySet(), doc.getPropertyNames());
  }

  public void testGetDocumentId() {
    DBHandle handle = new DBHandle(jsonDocument);
    String expected = "1";
    assertEquals(expected, handle.getDocumentId());
  }

  /**
   * Test that the JSON obect snapshot string is a limited subset of all the
   * properties. Only the docid and checksum should be included.
   */
  public void testToString() {
    DocumentHandle handle = new DBHandle(jsonDocument);
    String expected = "{\"google:docid\":\"1\",\"google:sum\":\"1234\"}";
    assertEquals(expected, handle.toString());
  }
}
