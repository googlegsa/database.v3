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
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.db.diffing.UrlDocumentBuilder.UrlType;

import java.util.HashMap;
import java.util.Map;

public class UrlDocumentBuilderTest extends DocumentBuilderFixture {
  /**
   * Test case for generateURLMetaFeed().
   */
  public final void testUrlDocument() throws Exception {
    String documentURL = "http://myhost/app/welcome.html";
    String versionColumn = "version";
    String versionValue = "2.3.4";

    Map<String, Object> rowMap = ImmutableMap.<String, Object>of(
        primaryKeyColumn, 1,
        dbContext.getDocumentURLField(), documentURL,
        versionColumn, versionValue);

    JsonDocument doc = getJsonDocument(
        new UrlDocumentBuilder(dbContext, UrlType.COMPLETE_URL), rowMap);
    assertEquals(versionValue, getProperty(doc, versionColumn));
    assertEquals(documentURL,
        getProperty(doc, SpiConstants.PROPNAME_SEARCHURL));
  }

  public final void testDocIdDocument() throws Exception {
    String docId = "index123.html";
    String versionColumn = "version";
    String versionValue = "2.3.4";

    Map<String, Object> rowMap = ImmutableMap.<String, Object>of(
        primaryKeyColumn, 2,
        dbContext.getDocumentIdField(), docId,
        versionColumn, versionValue);

    JsonDocument doc = getJsonDocument(
        new UrlDocumentBuilder(dbContext, UrlType.BASE_URL), rowMap);
    assertEquals(versionValue, getProperty(doc, versionColumn));
    assertEquals(dbContext.getBaseURL() + docId,
        getProperty(doc, SpiConstants.PROPNAME_SEARCHURL));
  }

  public void testDocumentUrlFieldValue() throws Exception {
    Object expectedUrl = "http://myhost/app/welcome.html";
    String originalName = dbContext.getDocumentURLField();
    Map<String, Object> row =
        ImmutableMap.of(primaryKeyColumn, 2, originalName, expectedUrl);

    testFieldName("documentURLField",
        new UrlDocumentBuilder(dbContext, UrlType.COMPLETE_URL), row,
        SpiConstants.PROPNAME_SEARCHURL, expectedUrl);
  }

  public void testDocumentIdFieldValue() throws Exception {
    Object docId = 3;
    String expectedUrl = dbContext.getBaseURL() + docId;
    String originalName = dbContext.getDocumentIdField();
    Map<String, Object> row =
        ImmutableMap.of(primaryKeyColumn, 2, originalName, docId);

    testFieldName("documentIdField",
        new UrlDocumentBuilder(dbContext, UrlType.BASE_URL), row,
        SpiConstants.PROPNAME_SEARCHURL, expectedUrl);
  }
}
