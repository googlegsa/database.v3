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

import com.google.common.collect.Maps;
import com.google.enterprise.connector.db.DBConnectorType;
import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBTestBase;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.SimpleTraversalContext;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.MimeTypeDetector;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.TraversalContextManager;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class DBSnapshotRepositoryTest extends DBTestBase {
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    runDBScript(CREATE_TEST_DB_TABLE);
    runDBScript(LOAD_TEST_DATA);
  }

  @Override
  protected void tearDown() throws Exception {
    try {
      runDBScript(DROP_TEST_DB_TABLE);
    } finally {
      super.tearDown();
    }
  }

  private DBSnapshotRepository getObjectUnderTest(
      Map<String, String> configMap) {
    // Connector manager does this in production.
    TraversalContext tc = new SimpleTraversalContext();
    MimeTypeDetector.setTraversalContext(tc);

    DBContext dbContext = getDbContext(configMap);

    TraversalContextManager traversalContextManager =
        new TraversalContextManager();
    traversalContextManager.setTraversalContext(tc);
    RepositoryHandler repositoryHandler =
        RepositoryHandler.makeRepositoryHandlerFromConfig(dbContext,
            traversalContextManager);
    return new DBSnapshotRepository(repositoryHandler);
  }

  /**
   * TODO: Add URL and DocId maps with the required configMap entries
   * and corresponding test methods.
   */
  private Map<String, String> getLobMap() {
    Map<String, String> newConfig = Maps.newHashMap(configMap);
    newConfig.put("extMetadataType", DBConnectorType.BLOB_CLOB);
    newConfig.put("lobField", "LNAME");
    return newConfig;
  }

  public void testIterator() {
    DBSnapshotRepository out = getObjectUnderTest(configMap);
    Iterator<? extends DocumentSnapshot> it = out.iterator();
    assertTrue(it.hasNext());
    do {
      it.next();
    } while (it.hasNext());

    try {
      it.next();
      fail("Expected a NoSuchElementException from an exhausted iterator.");
    } catch (NoSuchElementException expected) {
    }
  }

  public void testGetName() {
    assertEquals("com.google.enterprise.connector.db.diffing.DBSnapshotRepository",
        getObjectUnderTest(configMap).getName());
  }

  private DocumentSnapshot getSnapshotUnderTest(Map<String, String> configMap) {
    return getObjectUnderTest(configMap).iterator().next();
  }

  private DocumentSnapshot getDeserializedSnapshot(DocumentSnapshot snapshot) {
    String serialSnapshot = snapshot.toString();
    DocumentSnapshot deserialSnapshot =
        new DBSnapshotFactory().fromString(serialSnapshot);
    assertNotNull(deserialSnapshot);
    return deserialSnapshot;
  }

  private void testSnapshotLifecycle(Map<String, String> configMap)
      throws Exception {
    DocumentSnapshot snapshot = getSnapshotUnderTest(configMap);
    DocumentSnapshot deserialSnapshot = getDeserializedSnapshot(snapshot);

    // This is the core assertion, that a deserialized snapshot
    // compares as identical to the repository source snapshot.
    DocumentHandle update = snapshot.getUpdate(deserialSnapshot);
    if (update != null) {
      fail(update.toString());
    }

    assertEquals(snapshot.getDocumentId(), deserialSnapshot.getDocumentId());
    assertEquals(snapshot.toString(), deserialSnapshot.toString());
  }

  public void testSnapshotLifecycle_default() throws Exception {
    testSnapshotLifecycle(configMap);
  }

  public void testSnapshotLifecycle_lob() throws Exception {
    testSnapshotLifecycle(getLobMap());
  }

  private void testSnapshotUnsupportedOperation(Map<String, String> configMap)
      throws Exception {
    DocumentSnapshot snapshot = getSnapshotUnderTest(configMap);
    DocumentSnapshot deserialSnapshot = getDeserializedSnapshot(snapshot);

    try {
      DocumentHandle update = deserialSnapshot.getUpdate(snapshot);
      fail("Expected an UnsupportedOperationException exception");
    } catch (UnsupportedOperationException expected) {
    }
  }

  public void testSnapshotUnsupportedOperation_default() throws Exception {
    testSnapshotUnsupportedOperation(configMap);
  }

  public void testSnapshotUnsupportedOperation_lob() throws Exception {
    testSnapshotUnsupportedOperation(getLobMap());
  }

  public void testHandleLifecycle(Map<String, String> configMap)
      throws Exception {
    DocumentSnapshot snapshot = getSnapshotUnderTest(configMap);

    DocumentHandle handle = snapshot.getUpdate(null);
    assertNotNull(handle);

    Document doc = handle.getDocument();
    assertNotNull(doc);
    Set<String> propertyNames = doc.getPropertyNames();

    String serialHandle = handle.toString();
    DocumentHandle deserialHandle =
        new DBHandleFactory().fromString(serialHandle);
    assertNotNull(deserialHandle);
    assertEquals("document ID",
        handle.getDocumentId(), deserialHandle.getDocumentId());
    assertEquals("serialization not value preserving",
        serialHandle, deserialHandle.toString());

    Document recoveryDoc = deserialHandle.getDocument();
    assertNotNull(recoveryDoc);
    // This is the core assertion, that a document from a deserialized
    // handle has the same properties as the original handle from the
    // SnapshotRepository.
    assertEquals("document from deserialized handle",
        propertyNames, recoveryDoc.getPropertyNames());
  }

  public void testHandleLifecycle_default() throws Exception {
    testHandleLifecycle(configMap);
  }

  public void testHandleLifecycle_lob() throws Exception {
    testHandleLifecycle(getLobMap());
  }
}
