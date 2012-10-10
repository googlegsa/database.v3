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

import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.JsonDocumentUtil;
import com.google.enterprise.connector.db.diffing.JsonObjectUtil;
import com.google.enterprise.connector.db.diffing.MetadataDocumentBuilder;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import java.util.Map;

import junit.framework.TestCase;

public class JsonDocumentTest extends TestCase {

  private JsonObjectUtil jsonObjectUtil;

  protected void setUp() throws Exception {
    jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
  }

  public void testJsonDocument() {
    JsonDocument jsonDocument =
        new JsonDocument(jsonObjectUtil.getJsonObject());
    assertNotNull(jsonDocument);
  }

  public void testGetDocumentId() {
    JsonDocument jsonDocument =
        new JsonDocument(jsonObjectUtil.getJsonObject());
    assertEquals("1", jsonDocument.getDocumentId());
  }

  /**
   * Test that the JSON obect snapshot string is a limited subset of all the
   * properties.
   */
  public void testToJson() {
    String expected = "{\"google:docid\":\"1\"}";
    JsonDocument jsonDocument =
        new JsonDocument(jsonObjectUtil.getProperties(),
                         jsonObjectUtil.getJsonObject());
    assertEquals(expected, jsonDocument.toJson());
  }

  public void testFindProperty() {
    Map<String, Object> rowMap = TestUtils.getStandardDBRow();
    String[] primaryKeys = TestUtils.getStandardPrimaryKeys();
    try {
      ProductionTraversalContext context = new ProductionTraversalContext();
      JsonDocument.setTraversalContext(context);
      JsonDocument doc =
          new MetadataDocumentBuilder(DBTestBase.getMinimalDbContext())
          .fromRow(rowMap);

      assertEquals("MSxsYXN0XzAx", Value.getSingleValueString(doc,
          SpiConstants.PROPNAME_DOCID));
    
      String content = Value.getSingleValueString(doc, 
          SpiConstants.PROPNAME_CONTENT);
      assertNotNull(content);
      assertTrue(content.contains("id=1"));
      assertTrue(content.contains("lastName=last_01"));

      assertEquals("text/html", Value.getSingleValueString(doc,
          SpiConstants.PROPNAME_MIMETYPE));

      // Checksum should be hidden as a public property.
      assertNull(doc.findProperty(JsonDocumentUtil.ROW_CHECKSUM));
      
      // But the checksum should be included in the snapshot string.
      String expected = "{\"google:docid\":\"MSxsYXN0XzAx\","
          + "\"google:sum\":\"7ffd1d7efaf0d1ee260c646d827020651519e7b0\"}";
      assertEquals(expected, doc.toJson());
    } catch (DBException e) {
      fail("Could not generate Json document from row.");
    } catch (RepositoryException e) {
      fail("Could not generate Json document from row.");
    }
  }
}
