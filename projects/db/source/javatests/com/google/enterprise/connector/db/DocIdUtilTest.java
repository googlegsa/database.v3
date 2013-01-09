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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
   * Test getDocIdMap method.
   */
  public void testGetDocIdMap() {
    // doc Ids under test
    String docId1 = "1/Jan";
    String docId2 = "2/Feb";
    String docId3 = "3/March+Madness%21";
    // build Collection of doc Ids
    Collection<String> docIds = new ArrayList<String>();
    docIds.add(docId1);
    docIds.add(docId2);
    docIds.add(docId3);

    Map<String, String> docIdMap = DocIdUtil.getDocIdMap(docIds);
    assertNotNull(docIdMap);
    assertEquals(3, docIdMap.size());
    assertEquals(docId1, docIdMap.get("1,Jan"));
    assertEquals(docId2, docIdMap.get("2,Feb"));
    assertEquals(docId3, docIdMap.get("3,March Madness!"));
  }

  /**
   * Test generateDocId method.
   */
  public void testGenerateDocId() throws Exception {
    // Create a row representing a row in database table.
    Map<String, Object> row = new HashMap<String, Object>();

    // Array of primary key column names.
    String[] primaryKeys = {"pk1", "pk2", "pk3", "pk4", "pk5",
         "pk6", "pk7", "pk8", "pk9", "pk10", "pk11"};

    // Put "id" and "month" column values in map along with other columns.
    row.put("pk1", new Integer(123));
    row.put("pk2", new Long(-456));
    row.put("pk3", new Double(1234567890.1234567890));
    row.put("pk4", new BigDecimal("1234567890.1234567890E123"));
    row.put("pk5", new BigInteger("12345678901234567890"));
    row.put("pk6", new java.util.Date(1234567890));
    row.put("pk7", new Date(-1234567890));
    row.put("pk8", new Time(1234567890));
    Timestamp ts = new Timestamp(1234567890);
    ts.setNanos(123456789);
    row.put("pk9", ts);
    row.put("pk10", "Banana");
    row.put("pk11", "White space/punctuation!");
    row.put("col1", "value1");
    row.put("col2", "value2");
    row.put("col3", "value3");

    // Expected doc ID should be '/'-separated values of primary key columns,
    // with numbers, ISO8601 dates, and URL-encoded strings.
    String expectedDocId = "123/-456/1.2345678901234567E9/"
        + "1.2345678901234567890E132/12345678901234567890/"
        + "1970-01-14 22:56:07.890/1969-12-17/22:56:07/"
        + "1970-01-14 22:56:07.123456789/Banana/White+space%2Fpunctuation%21";

    String actualDocId = DocIdUtil.generateDocId(primaryKeys, row);
    assertNotNull(actualDocId);
    assertEquals(expectedDocId, actualDocId);
  }

  /**
   * Test generating and tokenizing a Docid that consists of a single integer.
   */
  public void testIntegerDocid() throws Exception {
    String[] primaryKey = { "ID" };
    Long id = 123456L;

    // Create a row representing a row in database table.
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("id", id);
    row.put("col1", "value1");
    row.put("col2", "value2");
    row.put("col3", "value3");

    String docId = DocIdUtil.generateDocId(primaryKey, row);
    assertNotNull(docId);
    assertEquals(id.toString(), docId);

    String[] tokens = DocIdUtil.tokenizeDocId(docId);
   assertNotNull(tokens);
    assertEquals(1, tokens.length);
    assertEquals(id.toString(), tokens[0]);
  }

  /**
   * Test tokenizeDocid.
   */
  public void testTokenizeDocid() throws Exception {
    String docId = "123/-456/1.2345678901234567E9/"
        + "1.2345678901234567890E132/12345678901234567890/"
        + "1970-01-14 22:56:07.890/1969-12-17/22:56:07/"
        + "1970-01-14 22:56:07.123456789/Banana/White+space%2Fpunctuation%21";

    String[] tokens = DocIdUtil.tokenizeDocId(docId);
    assertNotNull(tokens);
    assertEquals(11, tokens.length);
    assertEquals("123", tokens[0]);
    assertEquals("-456", tokens[1]);
    assertEquals("1.2345678901234567E9", tokens[2]);
    assertEquals("1.2345678901234567890E132", tokens[3]);
    assertEquals("12345678901234567890", tokens[4]);
    assertEquals("1970-01-14 22:56:07.890", tokens[5]);
    assertEquals("1969-12-17", tokens[6]);
    assertEquals("22:56:07", tokens[7]);
    assertEquals("1970-01-14 22:56:07.123456789", tokens[8]);
    assertEquals("Banana", tokens[9]);
    assertEquals("White space/punctuation!", tokens[10]);
  }

  /**
   * Test null primary key value in a compound docid.
   */
  public void testNullValue() throws Exception {
    String[] primaryKeys = new String[] { "PartNum", "Rev" };

    // Create a row representing a row in database table.
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("PartNum", "12345");
    row.put("Rev", null);

    String docId = DocIdUtil.generateDocId(primaryKeys, row);
    assertNotNull(docId);
    assertEquals("12345/", docId);

    String[] tokens = DocIdUtil.tokenizeDocId(docId);
    assertNotNull(tokens);
    assertEquals(2, tokens.length);
    assertEquals("12345", tokens[0]);
    assertEquals("", tokens[1]);

    row.put("PartNum", null);
    docId = DocIdUtil.generateDocId(primaryKeys, row);
    assertNotNull(docId);
    assertEquals(docId, "/", docId);
  }
}
