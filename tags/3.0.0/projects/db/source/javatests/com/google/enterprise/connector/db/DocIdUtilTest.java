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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class DocIdUtilTest extends TestCase {

  /**
   * Test getDocIdString method.
   */
  public void testGetDocIdString() {
    // doc Ids under test
    String docId1 = "1,Jan";
    String docId2 = "2,Feb";
    String docId3 = "3,Mar";
    // build Collection of doc Ids
    Collection<String> docIds = new ArrayList<String>();
    docIds.add(docId1);
    docIds.add(docId2);
    docIds.add(docId3);

    // build expected Doc Id String
    String expectedDocIdString = "'" + docId1 + "'," + "'" + docId2 + "',"
        + "'" + docId3 + "'";
    String actualDocIdString = DocIdUtil.getDocIdString(docIds);
    assertNotNull(actualDocIdString);
    assertEquals(expectedDocIdString, actualDocIdString);
  }

  /**
   * Test method decodeBase64String.
   */
  public void testDecodeBase64String() {
    String encodedString = "MSxtYXJjaA==";
    String expectedString = "1,march";
    String decodedString = null;
    decodedString = DocIdUtil.decodeBase64String(encodedString);
    assertNotNull(decodedString);
    assertEquals(expectedString, decodedString);
  }

  /**
   * Test getBase64EncodedString method.
   */
  public void testGetBase64EncodedString() {
    String testString = "1,march";
    String encodedString = DocIdUtil.getBase64EncodedString(testString);
    String expectedString = "MSxtYXJjaA";
    assertNotNull(encodedString);
    assertEquals(expectedString, encodedString);
  }

  /**
   * Test generateDocId method.
   */
  public void testGenerateDocId() {
    // Create a row representing a row in database table.
    Map<String, Object> row = new HashMap<String, Object>();

    // Array of primary key column names.
    String[] primaryKeys = new String[2];

    String pkCol1 = "id";
    String pkCol2 = "month";

    // Add "id" and "month" as primary key columns.
    primaryKeys[0] = pkCol1;
    primaryKeys[1] = pkCol2;

    // Put "id" and "month" column values in map along with other columns.
    row.put(pkCol1, 1);
    row.put(pkCol2, "Jan");
    row.put("col1", "value1");
    row.put("col2", "value2");
    row.put("col3", "value3");

    // Expected doc ID should be Base64-encoded, comma-separated values of
    // primary key columns, for example: "1,Jan".
    String expectedDocId = "MSxKYW4";
    String actualDocId = null;
    try {
      actualDocId = DocIdUtil.generateDocId(primaryKeys, row);
    } catch (DBException e) {
      fail("Exception occured while generating doc Id");
    }
    assertNotNull(actualDocId);
    assertEquals(expectedDocId, actualDocId);
  }
}
