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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.enterprise.connector.db.diffing.RepositoryHandler;
import com.google.enterprise.connector.db.diffing.RepositoryHandlerIterator;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;
import com.google.enterprise.connector.util.diffing.SnapshotRepositoryRuntimeException;

import junit.framework.TestCase;

import java.util.List;
import java.util.NoSuchElementException;

public class RepositoryHandlerIteratorTest extends TestCase {
  private DocumentSnapshot expectedSnapshot;
  private List<DocumentSnapshot> snapshotList;
  private List<DocumentSnapshot> emptySnapshotList;
  private RepositoryHandler repositoryHandler;
  private RepositoryHandlerIterator repositoryHandlerIterator;

  protected void setUp() throws Exception {
    expectedSnapshot = createMock(DocumentSnapshot.class);
    // Need at least two elements so we can have one more ready to return.
    snapshotList = ImmutableList.of(expectedSnapshot, expectedSnapshot);
    emptySnapshotList = ImmutableList.of();
    repositoryHandler = createMock(RepositoryHandler.class);
    repositoryHandlerIterator =
        new RepositoryHandlerIterator(repositoryHandler);
  }

  private void expectExecuteAndReturn(List<DocumentSnapshot> snapshotList) {
    expect(repositoryHandler.executeQueryAndAddDocs())
        .andReturn(snapshotList)
        .once();
    replay(repositoryHandler);
  }

  private void expectExecuteAndThrow() {
    expect(repositoryHandler.executeQueryAndAddDocs())
        .andThrow(new SnapshotRepositoryRuntimeException(
            "mock exception", new Exception("mock cause")))
        .once();
    replay(repositoryHandler);
  }

  private void readAllRows() {
    // RepositoryHandler implicitly restarts after an empty list. We
    // implicitly test that the RepositoryHandlerIterator stops on the
    // empty list by not expecting additional calls to
    // executeQueryAndAddDocs.
    expect(repositoryHandler.executeQueryAndAddDocs())
        .andReturn(snapshotList)
        .andReturn(emptySnapshotList);
    replay(repositoryHandler);

    for (DocumentSnapshot expected : snapshotList) {
      DocumentSnapshot snapshot = repositoryHandlerIterator.next();
      assertSame(expected, snapshot);
    }
  }

  public void testNext1() {
    expectExecuteAndReturn(snapshotList);

    DocumentSnapshot snapshot = repositoryHandlerIterator.next();
    assertSame(expectedSnapshot, snapshot);
    verify(repositoryHandler);
  }

  /** Tests exhausting the database and calling next too many times. */
  public void testNext2() {
    readAllRows();

    try {
      repositoryHandlerIterator.next();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }
    verify(repositoryHandler);
  }

  public void testNextThrows() {
    expectExecuteAndThrow();

    try {
      repositoryHandlerIterator.next();
      fail("Expected a SnapshotRepositoryRuntimeException");
    } catch (SnapshotRepositoryRuntimeException expected) {
    }
  }

  /** Scenario when the recordlist contains more records. */
  public void testHasNext1() {
    expectExecuteAndReturn(snapshotList);

    repositoryHandlerIterator.next(); // Read a snapshot to prime the list.
    verify(repositoryHandler); // Make sure the call was already made.

    assertTrue(repositoryHandlerIterator.hasNext());
  }

  /**
   * Scenario when the recordList does not contain more records but the
   * database result set does.
   */
  public void testHasNext2() {
    expectExecuteAndReturn(snapshotList);

    assertTrue(repositoryHandlerIterator.hasNext());
    verify(repositoryHandler);
  }

  /**
   * Scenario when the recordList as well as database resulset does not
   * contain any more records.
   */
  public void testHasNext3() {
    expectExecuteAndReturn(emptySnapshotList);

    assertFalse(repositoryHandlerIterator.hasNext());
    verify(repositoryHandler);
  }

  public void testHasNext4() {
    readAllRows();

    assertFalse(repositoryHandlerIterator.hasNext());
    verify(repositoryHandler);
  }

  public void testHasNextThrows() {
    expectExecuteAndThrow();

    try {
      repositoryHandlerIterator.hasNext();
      fail("Expected a SnapshotRepositoryRuntimeException");
    } catch (SnapshotRepositoryRuntimeException expected) {
    }
  }

  /**
   * Tests whether the iterator will restart.
   * RepositoryHandler.executeQueryAndAddDocs will return a non-empty
   * list after returning an empty one, so we have to make sure the
   * iterator stops on the empty list. Calling both hasNext and next
   * on the exhausted iterator should test that.
   */
  public void testReadAllRows() {
    readAllRows();

    assertFalse(repositoryHandlerIterator.hasNext());
    try {
      repositoryHandlerIterator.next();
      fail("Expected a NoSuchElementException");
    } catch (NoSuchElementException expected) {
    }
    verify(repositoryHandler);
  }

  /**
   * Tests that iterating over multiple batches returns each item in
   * the batches in turn.
   */
  public void testMultipleBatches() {
    List<List<DocumentSnapshot>> batches = Lists.newArrayList();
    List<DocumentSnapshot> snapshotList = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      List<DocumentSnapshot> batch = Lists.newArrayList();
      for (int j = 0; j < 3; j++) {
        DocumentSnapshot snapshot = createMock(DocumentSnapshot.class);
        batch.add(snapshot);
        snapshotList.add(snapshot);
      }
      batches.add(batch);
    }
    expect(repositoryHandler.executeQueryAndAddDocs());
    for (List<DocumentSnapshot> batch : batches) {
      expectLastCall().andReturn(batch);
    }
    expectLastCall().andReturn(emptySnapshotList);
    replay(repositoryHandler);

    for (DocumentSnapshot expected : snapshotList) {
      DocumentSnapshot snapshot = repositoryHandlerIterator.next();
      assertSame(expected, snapshot);
    }
    assertFalse(repositoryHandlerIterator.hasNext());
    verify(repositoryHandler);
  }
}
