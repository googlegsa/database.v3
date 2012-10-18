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

import com.google.enterprise.connector.db.diffing.DBClass;
import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.JsonObjectUtil;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import junit.framework.TestCase;

public class DBClassTest extends TestCase {

  JsonDocument jsonDocument;

  protected void setUp() throws Exception {
    JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
    jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
    jsonDocument = new JsonDocument(jsonObjectUtil.getJsonObject());
  }

  public void testFactoryFunction() {
    DBClass dbClass;
    dbClass = DBClass.factoryFunction.apply(jsonDocument);
    assertNotNull(dbClass);
  }

  public void testGetDocument() {
    DBClass dbClass;
    dbClass = new DBClass(jsonDocument);
    try {
      Document document = dbClass.getDocument();
      assertNotNull(document);
    } catch (RepositoryException e) {
      fail("Repository Exception in testGetDocument");
    }
  }

  public void testGetDocumentId() {
    DBClass dbClass;
    dbClass = new DBClass(jsonDocument);
    String expected = "1";
    assertEquals(expected, dbClass.getDocumentId());
  }

  public void testGetUpdate() {
    DocumentHandle documentHandle = new DBClass(jsonDocument);
    DocumentSnapshot documentSnapshot = new DBClass(jsonDocument);
    try {
      DocumentHandle actual = documentSnapshot.getUpdate(null);
      // The diffing framework sends in a null to indicate that it has not
      // seen this snapshot before. So we return the corresponding Handle
      // (in our case, the same object).
      assertEquals(documentHandle.getDocument(), actual.getDocument());

      // We just assume that if the serialized form is the same, then
      // nothing has changed.
      assertNull(documentSnapshot.getUpdate(documentSnapshot));

      // Something has changed, so return the corresponding handle.
      JsonObjectUtil jsonObjectUtil = new JsonObjectUtil();
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "2");
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
      JsonDocument jDoc = new JsonDocument(jsonObjectUtil.getJsonObject());
      DocumentSnapshot newdocumentSnapshot = new DBClass(jDoc);
      // Verify whether the changed property of the document has not been set.
      assertTrue(!jDoc.getChanged());
      DocumentHandle onGSA = documentSnapshot.getUpdate(newdocumentSnapshot);
      assertNotSame(documentHandle.getDocument(), onGSA);
      // Verify whether the changed property of the document has been set.
      JsonDocument jsonDoc = (JsonDocument) onGSA.getDocument();
      assertTrue(jsonDoc.getChanged());
    } catch (RepositoryException e) {
      fail("Repository Exception in testGetUpdate");
    }
  }

  public void testToString() {
    DBClass dbClass;
    String expected = "{\"google:ispublic\":\"false\",\"google:docid\":\"1\","
        + "\"google:mimetype\":\"text/plain\"}";
    dbClass = new DBClass(jsonDocument);
    assertEquals(expected, dbClass.toString());
  }
}
