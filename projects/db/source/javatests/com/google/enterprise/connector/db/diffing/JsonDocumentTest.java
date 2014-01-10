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

import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DBTestBase;
import com.google.enterprise.connector.db.InputStreamFactories;
import com.google.enterprise.connector.db.TestUtils;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;
import com.google.enterprise.connector.util.Base64;

import junit.framework.TestCase;

import java.util.Map;

public class JsonDocumentTest extends TestCase {

  private JsonObjectUtil jsonObjectUtil;

  protected void setUp() throws Exception {
    jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "B/1");
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
    assertEquals("B/1", jsonDocument.getDocumentId());
  }

  /** Tests that the JSON object string contains all the properties. */
  public void testToJson() {
    String expected = "{\"google:ispublic\":\"false\",\"google:docid\":\"B/1\","
        + "\"google:mimetype\":\"text/plain\"}";
    JsonDocument jsonDocument =
        new JsonDocument(jsonObjectUtil.getProperties(),
                         jsonObjectUtil.getJsonObject());
    assertEquals(expected, jsonDocument.toJson());
  }

  public void testFindProperty() throws Exception {
    Map<String, Object> rowMap = TestUtils.getStandardDBRow();
    try {
      ProductionTraversalContext context = new ProductionTraversalContext();
      JsonDocument.setTraversalContext(context);
      JsonDocument doc = DocumentBuilderFixture.getJsonDocument(
          new MetadataDocumentBuilder(DBTestBase.getMinimalDbContext()),
          rowMap);

      assertEquals("BF/1/last_01", Value.getSingleValueString(doc,
          SpiConstants.PROPNAME_DOCID));

      Value contentValue = Value.getSingleValue(doc,
          SpiConstants.PROPNAME_CONTENT);
      assertNotNull(contentValue);
      String content = new String(
          Base64.decode(InputStreamFactories.toString(contentValue)), "UTF-8");
      assertEquals(SpiConstants.ContentEncoding.BASE64BINARY.toString(),
          Value.getSingleValueString(
              doc, SpiConstants.PROPNAME_CONTENT_ENCODING));
      assertTrue(content, content.contains("id=1"));
      assertTrue(content, content.contains("lastName=last_01"));

      assertEquals("text/html", Value.getSingleValueString(doc,
          SpiConstants.PROPNAME_MIMETYPE));

      // Checksum should be hidden as a public property and in the JSON string.
      assertNull(doc.findProperty(DocumentBuilder.ROW_CHECKSUM));
      assertEquals(doc.toJson(), -1, doc.toJson().indexOf("google:sum"));
    } catch (DBException e) {
      fail("Could not generate Json document from row.");
    } catch (RepositoryException e) {
      fail("Could not generate Json document from row.");
    }
  }
}
