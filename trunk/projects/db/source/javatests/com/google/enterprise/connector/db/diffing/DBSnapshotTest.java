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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import junit.framework.TestCase;

public class DBSnapshotTest extends TestCase {
  private DocumentBuilder builder;
  private DocumentBuilder.DocumentHolder holder;
  private DocumentSnapshot documentSnapshot;

  protected void setUp() throws Exception {
    // This is a partial mock, only mocking the getDocumentHandle method
    // (and getDocumentSnapshot, which we're not using here).
    builder = createMock(DocumentBuilder.class);

    String docId = "1";
    String mimeType = "text/plain";
    String checksum = "1234";
    String jsonString = builder.getJsonString(docId, checksum);
    holder = new DocumentBuilder.DocumentHolder(builder, null, docId,
        new DocumentBuilder.ContentHolder("hello, world", checksum, mimeType));

    documentSnapshot = new DBSnapshot("1", jsonString, holder);
  }

  public void testGetDocumentId() {
    DBSnapshot snapshot = new DBSnapshot("1", "", null);
    assertEquals("1", snapshot.getDocumentId());
  }

  public void testGetUpdateNewDocument() throws Exception {
    // The diffing framework sends in a null to indicate that it has not
    // seen this snapshot before.
    // Assert that our DocumentHolder is used to create the DocumentHandle.
    expect(builder.getDocumentHandle(same(holder))).andReturn(null);
    replay(builder);
    DocumentHandle actual = documentSnapshot.getUpdate(null);
    verify(builder);
  }

  public void testGetUpdateNoChange() throws Exception {
    String serialSnapshot = documentSnapshot.toString();
    DocumentSnapshot deserialSnapshot =
        new DBSnapshotFactory().fromString(serialSnapshot);
    assertNull(documentSnapshot.getUpdate(deserialSnapshot));
  }

  public void testGetUpdateChangedDocument() throws Exception {
    DocumentSnapshot onGsa = new DBSnapshot(
        builder.getJsonString(documentSnapshot.getDocumentId(), "9999"));

    // Assert that our DocumentHolder is used to create the DocumentHandle.
    expect(builder.getDocumentHandle(same(holder))).andReturn(null);
    replay(builder);
    DocumentHandle update = documentSnapshot.getUpdate(onGsa);
    verify(builder);
  }

  /**
   * Test that the JSON object snapshot string is a limited subset of all the
   * properties. Only the docid and checksum should be included.
   */
  public void testToString() {
    String expected = "{\"google:docid\":\"1\",\"google:sum\":\"1234\"}";
    assertEquals(expected, documentSnapshot.toString());
  }

  private void compareDocids(String lesserId, String greaterId)
      throws Exception {
    DBSnapshot snapshot1 = new DBSnapshot(lesserId, "", null);
    DBSnapshot snapshot2 = new DBSnapshot(greaterId, "", null);
    assertEquals(0, snapshot1.compareTo(snapshot1));
    assertEquals(0, snapshot2.compareTo(snapshot2));
    assertTrue(snapshot1.compareTo(snapshot2) < 0);
    assertTrue(snapshot2.compareTo(snapshot1) > 0);
  }

  public void testCompareIntegerDocids() throws Exception {
    compareDocids("-11", "-1");
    compareDocids("-11", "1");
    compareDocids("-11", "10");
    compareDocids("-1", "1");
    compareDocids("-1", "10");
    compareDocids("1", "2");
    compareDocids("1", "10");
    compareDocids("2", "10");
  }

  public void testCompareBigIntegerDocids() throws Exception {
    compareDocids("-123456789012345678901", "123456789012345678901");
    compareDocids("-123456789012345678901", "-23456789012345678901");
    compareDocids("23456789012345678901", "123456789012345678901");
    compareDocids("123456789012345678901", "123456789012345678902");
    compareDocids("123456789012345678901", "1234567890123456789000");
    compareDocids("123456789012345678901", "1234567890123456789010");
  }

  public void testCompareDoubleDocids() throws Exception {
    compareDocids("-1234E10", "-1234E-10");
    compareDocids("-1234E10", "1234E-10");
    compareDocids("-1234E10", "1234E10");
    compareDocids("-1234E-10", "1234E-10");
    compareDocids("1234E-10", "1234E10");
    compareDocids("1234E10", "12345E9");
    compareDocids("12345E9", "12345E10");
    compareDocids("12345E10", "12346E10");
    compareDocids("2345E9", "1234E10");
  }

  public void testCompareBigDecimalDocids() throws Exception {
    compareDocids("-12345678901234567890E10", "-12345678901234567890E-10");
    compareDocids("-12345678901234567890E10", "12345678901234567890E-10");
    compareDocids("-12345678901234567890E10", "12345678901234567890E10");
    compareDocids("12345678901234567890E-10", "12345678901234567890E10");
    compareDocids("12345678901234567891E9", "12345678901234567890E10");
    compareDocids("12345678901234567890E10", "12345678912345678900E10");
    compareDocids("12345678901234567890E10", "12345678901234567890E11");
    compareDocids("2345678901234567890E10", "1234567890123456789E11");
  }

  public void testCompareStringDocids() throws Exception {
    compareDocids("apple", "banana");
    compareDocids("Banana", "apple");
    compareDocids("APPLE", "apple");
    compareDocids("a+b", "a*b");
    compareDocids("a+b", "a-b");
    compareDocids("a+b", "a%2Bb");    
    compareDocids("a-b", "a%2Fb");
    compareDocids("1000000+Years+BC", "20000+Leagues+Under+the+Sea");
  }

  public void testCompareDateTimeDocids() throws Exception {
    compareDocids("1969-07-20", "1972-12-11");
    compareDocids("1969-07-20 20:17:40", "1972-12-11 19:54:57");
    compareDocids("01:23:45", "18:00:00");
    compareDocids("1972-12-11 19:54:57.123", "1972-12-11 19:54:57.789");
    compareDocids("1969-07-20 20:17:40.123456789",
                  "1972-12-11 19:54:57.987654321");
  }

  public void testCompareCompoundDocids() throws Exception {
    compareDocids("Apollo/8/1968-12-21 12:51:00",
                  "Apollo/11/1969-07-16 13:32:00");
    compareDocids("Apollo/11/1969-07-16 13:32:00",
                  "Mercury/6/1962-02-20 14:47:39");
    compareDocids("1962-02-20 14:47:39/Mercury/6",
                  "1969-07-16 13:32:00/Apollo/11");
    compareDocids("Apollo/11/Aldrin", "Apollo/11/Armstrong");
    compareDocids("Armstrong/Lance", "Armstrong/Stretch");
    compareDocids("Mac+OS/9", "Mac+OS/X");
  }
}
