// Copyright 2013 Google Inc. All Rights Reserved.
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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.common.base.Charsets;
import com.google.enterprise.connector.db.diffing.DigestContentHolder;
import com.google.enterprise.connector.util.InputStreamFactory;

import junit.framework.TestCase;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class LobTypeHandlerTest extends TestCase {
  private static final String CONTENT = "hello, world";
  private static final long CONTENT_LENGTH = CONTENT.length();

  /** Tests the type strategy when Blob.free throws an exception. */
  private void testBlobAndThrow(LobTypeHandler.Strategy objectUnderTest,
      final Throwable throwable) throws SQLException {
    byte[] content = CONTENT.getBytes(Charsets.UTF_8);
    Blob blob = createMock(Blob.class);
    expect(blob.length()).andReturn(CONTENT_LENGTH).anyTimes();
    expect(blob.getBytes(anyInt(), anyInt())).andReturn(content).anyTimes();
    blob.free();
    expectLastCall().andThrow(throwable).atLeastOnce();
    replay(blob);

    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getBlob(anyInt())).andReturn(blob).anyTimes();
    replay(rs);

    testContentWithFree(objectUnderTest, blob, rs);
  }

  /** Tests the type strategy when Clob.free throws an exception. */
  private void testClobAndThrow(LobTypeHandler.Strategy objectUnderTest,
      final Throwable throwable) throws SQLException {
    StringReader content = new StringReader(CONTENT);
    Clob clob = createMock(Clob.class);
    expect(clob.length()).andReturn(CONTENT_LENGTH).anyTimes();
    expect(clob.getCharacterStream()).andReturn(content).anyTimes();
    clob.free();
    expectLastCall().andThrow(throwable).atLeastOnce();
    replay(clob);

    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getClob(anyInt())).andReturn(clob).anyTimes();
    replay(rs);

    testContentWithFree(objectUnderTest, clob, rs);
  }

  private void testContentWithFree(LobTypeHandler.Strategy objectUnderTest,
      Object lobMock, ResultSet rs) throws SQLException {
    assertEquals(CONTENT,
        new String(objectUnderTest.getBytes(rs, 1), Charsets.UTF_8));
    verify(lobMock);
  }

  public void testBlobFreeUnsupported() throws SQLException {
    testBlobAndThrow(new BlobTypeStrategy(),
        new UnsupportedOperationException());
  }

  public void testBlobFreeLinkageError() throws SQLException {
    testBlobAndThrow(new BlobTypeStrategy(), new AbstractMethodError());
  }

  public void testClobFreeUnsupported() throws SQLException {
    testClobAndThrow(new ClobTypeStrategy(),
        new UnsupportedOperationException());
  }

  public void testClobFreeLinkageError() throws SQLException {
    testClobAndThrow(new ClobTypeStrategy(), new AbstractMethodError());
  }

  /**
   * Tests null values of the given type.
   *
   * @param rs a mock ResultSet that returns null values for the given type
   * @param jdbcType the column type ({@code Types.BLOB}, etc.)
   */
  private void testNull(ResultSet rs, int jdbcType)
      throws SQLException, IOException {
    ResultSetMetaData rsmd = createMock(ResultSetMetaData.class);
    expect(rsmd.getColumnType(anyInt())).andReturn(jdbcType).atLeastOnce();
    expect(rs.getMetaData()).andReturn(rsmd).atLeastOnce();
    replay(rsmd, rs);

    DigestContentHolder holder = new LobTypeHandler().getNullableResult(rs, 1);
    verify(rsmd, rs);

    // SQL NULLs are rendered as empty byte arrays for easier handling.
    Object content = holder.getContent();
    assertTrue(content.getClass().toString(),
        content instanceof InputStreamFactory);
    assertEquals(-1, ((InputStreamFactory) content).getInputStream().read());
  }

  public void testNullBlob() throws SQLException, IOException {
    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getBlob(anyInt())).andReturn(null).atLeastOnce();
    testNull(rs, Types.BLOB);
  }

  public void testNullClob() throws SQLException, IOException {
    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getClob(anyInt())).andReturn(null).atLeastOnce();
    testNull(rs, Types.CLOB);
  }

  public void testNullBinary() throws SQLException, IOException {
    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getBytes(anyInt())).andReturn(null).atLeastOnce();
    testNull(rs, Types.VARBINARY);
  }

  public void testNullChar() throws SQLException, IOException {
    ResultSet rs = createMock(ResultSet.class);
    expect(rs.getString(anyInt())).andReturn(null).atLeastOnce();
    testNull(rs, Types.VARCHAR);
  }
}
