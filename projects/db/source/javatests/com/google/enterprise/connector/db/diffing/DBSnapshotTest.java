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
   * Test that the JSON obect snapshot string is a limited subset of all the
   * properties. Only the docid and checksum should be included.
   */
  public void testToString() {
    String expected = "{\"google:docid\":\"1\",\"google:sum\":\"1234\"}";
    assertEquals(expected, documentSnapshot.toString());
  }
}