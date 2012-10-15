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

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import junit.framework.TestCase;

public class DBSnapshotTest extends TestCase {

  JsonDocument jsonDocument;

  protected void setUp() throws Exception {
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
    jsonObjectUtil.setProperty(DocumentBuilder.ROW_CHECKSUM, "1234");
    jsonDocument = new JsonDocument(jsonObjectUtil.getProperties(),
                                    jsonObjectUtil.getJsonObject());
  }

  public void testFactoryFunction() {
    DBSnapshot snapshot = DBSnapshot.factoryFunction.apply(jsonDocument);
    assertNotNull(snapshot);
  }

  public void testGetDocumentId() {
    DBSnapshot snapshot = new DBSnapshot(jsonDocument);
    String expected = "1";
    assertEquals(expected, snapshot.getDocumentId());
  }

  public void testGetUpdateNewDocument() throws Exception {
    DocumentSnapshot documentSnapshot = new DBSnapshot(jsonDocument);
    DocumentHandle actual = documentSnapshot.getUpdate(null);
    // The diffing framework sends in a null to indicate that it has not
    // seen this snapshot before. So we return the corresponding handle,
    // which in our case contains the same document object.
    assertSame(jsonDocument, actual.getDocument());
  }

  public void testGetUpdateNoChange() throws Exception {
    DocumentSnapshot documentSnapshot = new DBSnapshot(jsonDocument);
    String serialSnapshot = documentSnapshot.toString();
    DocumentSnapshot deserialSnapshot =
        new DBSnapshotFactory().fromString(serialSnapshot);
    assertNull(documentSnapshot.getUpdate(deserialSnapshot));
  }

  public void testGetUpdateChangedDocument() throws Exception {
    // This represents the serialized snapshot that the GSA knows about.
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
    // Checksum change indicates document changed.
    jsonObjectUtil.setProperty(DocumentBuilder.ROW_CHECKSUM, "9999");
    DocumentSnapshot onGsa =
        new DBSnapshot(jsonObjectUtil.getJsonObject().toString());

    // This represents the document as found in the repository.
    DocumentSnapshot documentSnapshot = new DBSnapshot(jsonDocument);
    // Verify whether the changed property of the document has not been set.
    assertFalse(jsonDocument.getChanged());

    DocumentHandle update = documentSnapshot.getUpdate(onGsa);
    assertSame(update.getDocument(), jsonDocument);
    // Verify whether the changed property of the document has been set.
    assertTrue(jsonDocument.getChanged());
  }

  /**
   * Test that the JSON obect snapshot string is a limited subset of all the
   * properties. Only the docid and checksum should be included.
   */
  public void testToString() {
    DBSnapshot snapshot = new DBSnapshot(jsonDocument);
    String expected = "{\"google:docid\":\"1\",\"google:sum\":\"1234\"}";
    assertEquals(expected, snapshot.toString());
  }
}
