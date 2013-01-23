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
    String docId1 = "MSxKYW4"; // Legacy docid Base64 encoded "1,Jan";
    String docId2 = "BF/2/Feb";
    String docId3 = "BF/3/March+Madness%21";
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
    assertEquals(docIdMap.toString(), docId3, docIdMap.get("3,March Madness!"));
  }

  /**
   * Test generateDocId method.
   */
  public void testGenerateDocId() throws Exception {
    // Create a row representing a row in database table.
    Map<String, Object> row = new HashMap<String, Object>();

    // Array of primary key column names.
    String[] primaryKeys = { "pk1", "pk2", "pk3", "pk4", "pk5",
        "pk6", "pk7", "pk8", "pk9", "pk10", "pk11", "pk12", "pk13" };

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
    row.put("pk12", null);
    row.put("pk13", new Boolean(true));
    row.put("col1", "value1");
    row.put("col2", "value2");
    row.put("col3", "value3");

    // Expected doc ID should be '/'-separated values of primary key columns,
    // with numbers, ISO8601 dates, and URL-encoded strings.
    String expectedDocId = "BBCEDGIJHFFAK/123/-456/1.2345678901234567E9/"
        + "1.2345678901234567890E132/12345678901234567890/"
        + "1970-01-14 22:56:07.890/1969-12-17/22:56:07/"
        + "1970-01-14 22:56:07.123456789/Banana/White+space%2Fpunctuation%21//"
        + "true";

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
    assertEquals("B/123456", docId);
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
    assertEquals("FA/12345/", docId);

    row.put("PartNum", null);
    docId = DocIdUtil.generateDocId(primaryKeys, row);
    assertNotNull(docId);
    assertEquals(docId, "AA//", docId);
  }

  private void compareDocids(String lesserId, String greaterId)
      throws Exception {
    assertEquals(0, DocIdUtil.compare(lesserId, lesserId));
    assertEquals(0, DocIdUtil.compare(greaterId, greaterId));
    assertTrue(DocIdUtil.compare(lesserId, greaterId) < 0);
    assertTrue(DocIdUtil.compare(greaterId, lesserId) > 0);
  }

  public void testCompareIntegerDocids() throws Exception {
    compareDocids("B/-11", "B/-1");
    compareDocids("B/-11", "B/1");
    compareDocids("B/-11", "B/10");
    compareDocids("B/-1", "B/1");
    compareDocids("B/-1", "B/10");
    compareDocids("B/1", "B/2");
    compareDocids("B/1", "B/10");
    compareDocids("B/2", "B/10");
  }

  public void testCompareBigIntegerDocids() throws Exception {
    compareDocids("D/-123456789012345678901", "D/123456789012345678901");
    compareDocids("D/-123456789012345678901", "D/-23456789012345678901");
    compareDocids("D/23456789012345678901", "D/123456789012345678901");
    compareDocids("D/123456789012345678901", "D/123456789012345678902");
    compareDocids("D/123456789012345678901", "D/1234567890123456789000");
    compareDocids("D/123456789012345678901", "D/1234567890123456789010");
  }

  public void testCompareDoubleDocids() throws Exception {
    compareDocids("C/-1234E10", "C/-1234E-10");
    compareDocids("C/-1234E10", "C/1234E-10");
    compareDocids("C/-1234E10", "C/1234E10");
    compareDocids("C/-1234E-10", "C/1234E-10");
    compareDocids("C/1234E-10", "C/1234E10");
    compareDocids("C/1234E10", "C/12345E9");
    compareDocids("C/12345E9", "C/12345E10");
    compareDocids("C/12345E10", "C/12346E10");
    compareDocids("C/2345E9", "C/1234E10");
  }

  public void testCompareBigDecimalDocids() throws Exception {
    compareDocids("E/234", "E/1234");
    compareDocids("E/-12345678901234567890E10", "E/-12345678901234567890E-10");
    compareDocids("E/-12345678901234567890E10", "E/12345678901234567890E-10");
    compareDocids("E/-12345678901234567890E10", "E/12345678901234567890E10");
    compareDocids("E/12345678901234567890E-10", "E/12345678901234567890E10");
    compareDocids("E/12345678901234567891E9", "E/12345678901234567890E10");
    compareDocids("E/12345678901234567890E10", "E/12345678912345678900E10");
    compareDocids("E/12345678901234567890E10", "E/12345678901234567890E11");
    compareDocids("E/2345678901234567890E10", "E/1234567890123456789E11");
  }

  public void testCompareStringDocids() throws Exception {
    compareDocids("F/apple", "F/banana");
    compareDocids("F/Banana", "F/apple");
    compareDocids("F/APPLE", "F/apple");
    compareDocids("F/a+b", "F/a*b");
    compareDocids("F/a+b", "F/a-b");
    compareDocids("F/a+b", "F/a%2Bb");    
    compareDocids("F/a-b", "F/a%2Fb");
    compareDocids("F/1000000+Years+BC", "F/20000+Leagues+Under+the+Sea");
  }

  public void testCompareDateTimeDocids() throws Exception {
    compareDocids("I/1969-07-20", "I/1972-12-11");
    compareDocids("G/1969-07-20 20:17:40", "G/1972-12-11 19:54:57");
    compareDocids("J/01:23:45", "J/18:00:00");
    compareDocids("G/1972-12-11 19:54:57.123", "G/1972-12-11 19:54:57.789");
    compareDocids("H/1969-07-20 20:17:40.123456789",
                  "H/1972-12-11 19:54:57.987654321");
  }

  public void testCompareBooleanDocids() throws Exception {
    compareDocids("K/false", "K/true");
  }

  public void testCompareCompoundDocids() throws Exception {
    compareDocids("FBG/Apollo/8/1968-12-21 12:51:00",
                  "FBG/Apollo/11/1969-07-16 13:32:00");
    compareDocids("FBG/Apollo/11/1969-07-16 13:32:00",
                  "FBG/Mercury/6/1962-02-20 14:47:39");
    compareDocids("GFB/1962-02-20 14:47:39/Mercury/6",
                  "GFB/1969-07-16 13:32:00/Apollo/11");
    compareDocids("FBF/Apollo/11/Aldrin", "FBF/Apollo/11/Armstrong");
    compareDocids("FF/Armstrong/Lance", "FF/Armstrong/Stretch");
    compareDocids("FF/Mac+OS/9", "FF/Mac+OS/X");
  }

  public void testCompareMixedNumericDocids() throws Exception {
    compareDocids("B/-1234", "C/-1234E-10");
    compareDocids("C/-1234E10", "E/1234E-10");
    compareDocids("B/234", "D/1234567890");
    compareDocids("E/1234567890E-10", "D/1234567890");
  }

  public void testCompareMixedDocids() throws Exception {
    compareDocids("B/1234", "F/124");
    compareDocids("I/1969-07-20", "E/196907E-20");
    compareDocids("F/123456E+10", "C/123456E+10");
    compareDocids("H/1969-07-20 20:17:40.123456789",
                  "G/1972-12-11 19:54:57.789");
  }

  public void testCompareNullsInDocids() throws Exception {
    compareDocids("A/", "F/Hi");
    compareDocids("FA/Hi/", "FB/Hi/-123456");
    compareDocids("FAB/Hi//-12345", "FAB/Hi//23456");
  }

  public void testCompareLegacyDocid() throws Exception {
    // Legacy docids always sort lower.
    compareDocids("MSxKYW4", "I/1969-07-20");
  }
}
