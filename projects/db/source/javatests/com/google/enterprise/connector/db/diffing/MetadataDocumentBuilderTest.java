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

import com.google.enterprise.connector.db.TestUtils;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;

import java.util.Map;
import java.util.logging.Logger;

public class MetadataDocumentBuilderTest extends DocumentBuilderFixture {
  private static final Logger LOG =
      Logger.getLogger(MetadataDocumentBuilderTest.class.getName());

  /**
   * Test for converting DB row to DB Doc.
   */
  public final void testRowToDoc() throws Exception {
    Map<String, Object> rowMap = TestUtils.getStandardDBRow();
    MetadataDocumentBuilder builder =
        new MetadataDocumentBuilder(getMinimalDbContext());
    JsonDocument doc = getJsonDocument(builder, rowMap);
    for (String propName : doc.getPropertyNames()) {
      LOG.info(propName + ":    " + getProperty(doc, propName));
    }
    assertEquals("1/last_01", getProperty(doc, SpiConstants.PROPNAME_DOCID));
    Value contentValue = Value.getSingleValue(doc,
        SpiConstants.PROPNAME_CONTENT);
    assertNotNull(contentValue);
    String content = InputStreamFactories.toString(contentValue);
    assertTrue(content.contains("id=1"));
    assertTrue(content.contains("lastName=last_01"));
    assertEquals("text/html", getProperty(doc, SpiConstants.PROPNAME_MIMETYPE));

    // Checksum should be hidden as a public property and in the JSON string.
    assertNull(doc.findProperty(DocumentBuilder.ROW_CHECKSUM));
    assertEquals(doc.toJson(), -1, doc.toJson().indexOf("google:sum"));
  }
}
