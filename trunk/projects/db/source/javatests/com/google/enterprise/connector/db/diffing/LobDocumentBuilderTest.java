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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.testing.MockClient;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.traversal.FileSizeLimitInfo;
import com.google.enterprise.connector.traversal.MimeTypeMap;
import com.google.enterprise.connector.util.MimeTypeDetector;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.serial.SerialClob;

public class LobDocumentBuilderTest extends DocumentBuilderFixture {
  public void testPrimaryKeySkipped() throws Exception {
    Object docid = 2;
    String expectedDocid = "B/" + docid;
    Object clobContent = "hello, world";
    Map<String, Object> row = ImmutableMap.of(
        primaryKeyColumn, docid, dbContext.getLobField(), clobContent);
    MimeTypeDetector.setTraversalContext(context);

    FieldNameBean bean = new FieldNameBean(dbContext) {
        public String get() { return primaryKeyColumn; }
        public void set(String value) { dbContext.setPrimaryKeys(value); }
        public List<String> getNameVariations() {
          return ImmutableList.of("id", "Id", "ID", "  id", " iD  "); } };

    // We don't care about the property value testing, just test docid.
    testFieldName("primaryKey", bean,
        new LobDocumentBuilder(dbContext, context), row,
        SpiConstants.PROPNAME_DOCID, expectedDocid);
  }

  public void testLobFieldValue() throws Exception {
    Object clobContent = "hello, world";
    String originalName = dbContext.getLobField();
    Map<String, Object> row =
        ImmutableMap.of(primaryKeyColumn, 2, originalName, clobContent);
    MimeTypeDetector.setTraversalContext(context);

    testFieldName("lobField", new LobDocumentBuilder(dbContext, context),
        row, SpiConstants.PROPNAME_CONTENT, clobContent);
  }

  /**
   * Tests that the lobField is filtered from the XML map, which calls
   * toString on the map values. In order to avoid extraneous calls to
   * toString in LobDocumentBuilder.getBinaryContent when extracting
   * the LOB value, we need to supply it as either a String (which we
   * can't override toString on) or a Clob. EasyMock cannot override
   * toString, so I picked a random class I found that implements Clob
   * so I don't have to, namely javax.sql.rowset.serial.SerialClob.
   */
  public void testLobFieldFiltered() throws Exception {
    Object docid = 2;
    String expectedDocid = "B/" + docid;
    Object clobContent = new SerialClob("hello, world".toCharArray()) {
        public String toString() { throw new IllegalStateException(); } };
    String originalName = dbContext.getLobField();
    Map<String, Object> row =
        ImmutableMap.of(primaryKeyColumn, docid, originalName, clobContent);
    MimeTypeDetector.setTraversalContext(context);

    // We don't care about the property value testing, just test docid.
    testFieldName("lobField", new LobDocumentBuilder(dbContext, context),
        row, SpiConstants.PROPNAME_DOCID, expectedDocid);
  }

  public void testFetchUrlFieldValue() throws Exception {
    Object expectedUrl = "http://example.com/2";
    String originalName = dbContext.getFetchURLField();
    Map<String, Object> row =
        ImmutableMap.of(primaryKeyColumn, 2, originalName, expectedUrl);
    MimeTypeDetector.setTraversalContext(context);

    testFieldName("fetchURLField", new LobDocumentBuilder(dbContext, context),
        row, SpiConstants.PROPNAME_DISPLAYURL, expectedUrl);
  }

  /**
   * When fetchURLField is null or whitespace, the connector should
   * return a dbConnector display URL, and it should not try to call
   * row.get(String) with the null key (Map implementations are not
   * guaranteed to support null keys).
   */
  public void testFetchUrlFieldNull() throws Exception {
    DocumentBuilder builder = new LobDocumentBuilder(dbContext, context);
    Object docid = 2;
    String expectedUrl = builder.getDisplayUrl("B/" + docid);
    dbContext.setFetchURLField(null);
    Map<String, Object> row = new HashMap<String, Object>() {
      @Override
      public Object get(Object key) {
        if (key == null) {
          throw new NullPointerException();
        } else {
          return super.get(key);
        }
      }
    };
    row.put(primaryKeyColumn, docid);
    MimeTypeDetector.setTraversalContext(context);

    testFieldName("fetchURLField", builder, row,
        SpiConstants.PROPNAME_DISPLAYURL, expectedUrl);
  }

  private Map<String, Object> getLargeObjectRow() {
    Map<String, Object> rowMap = new HashMap<String, Object>();
    // Define common test data.
    String versionColumn = "version";
    String versionValue = "2.3.4";
    rowMap.put(primaryKeyColumn, 1);
    rowMap.put(versionColumn, versionValue);
    return rowMap;
  }

  public void testStringClobDocument() throws Exception {
    String clobContent = getClobContent();
    testCLOBDataScenarios(clobContent, clobContent);
  }

  public void testCharArrayClobDocument() throws Exception {
    String clobContent = getClobContent();
    testCLOBDataScenarios(clobContent.toCharArray(), clobContent);
  }

  public void testSqlClobDocument() throws Exception {
    String clobContent = getClobContent();
    long clobLength = clobContent.length();
    StringReader clobReader = new StringReader(clobContent);

    Clob clob = createMock(Clob.class);
    expect(clob.length()).andReturn(clobLength).anyTimes();
    expect(clob.getCharacterStream()).andReturn(clobReader).anyTimes();
    replay(clob);

    testCLOBDataScenarios(clob, clobContent);
  }

  private String getClobContent() {
    // Define CLOB data larger than the FileBackedOutputStream will
    // hold in memory for this test case.
    return MockClient.getClob(100000);
  }

  /**
   * Test scenarios for CLOB data types.
   */
  public void testCLOBDataScenarios(Object clobValue, String clobContent)
        throws IOException, DBException, RepositoryException {
    Map<String, Object> rowMap = getLargeObjectRow();
    rowMap.put(dbContext.getLobField(), clobValue);

    MimeTypeDetector.setTraversalContext(context);
    FileSizeLimitInfo fileSizeLimitInfo = new FileSizeLimitInfo();
    fileSizeLimitInfo.setMaxDocumentSize(5);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);

    JsonDocument clobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
    // As the size of the document is more than supported, clobDoc should have
    // null value.
    assertNotNull(clobDoc);
    assertNull(Value.getSingleValue(clobDoc, SpiConstants.PROPNAME_CONTENT));

    // Increase the maximum supported size of the document.
    fileSizeLimitInfo.setMaxDocumentSize(1024 * 1024);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);
    clobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
    assertNotNull(clobDoc);

    // Test scenario:- this doc will have column name "version" as
    // metadata key and value will be "2.3.4".
    assertEquals(rowMap.get("version"), getProperty(clobDoc, "version"));

    // Test scenario:- the content of this document will be same as the
    // content of CLOB column.
    assertEquals(clobContent,
        getProperty(clobDoc, SpiConstants.PROPNAME_CONTENT));

    // The MIME type of the content should have been automatically determined.
    assertEquals("text/plain",
        getProperty(clobDoc, SpiConstants.PROPNAME_MIMETYPE));

    // Test scenario:- primary key column should be excluded while
    // indexing external metadata.
    assertNull(getProperty(clobDoc, primaryKeyColumn));
  }

  private byte[] getBlobContent() {
    // Gets a randomized 100-byte array that resolves as application/pdf.
    return MockClient.getBlob(100, true);
  }

  private Map<String, Object> getBlobRow(Object blobContent) {
    Map<String, Object> rowMap = getLargeObjectRow();
    rowMap.put(dbContext.getLobField(), blobContent);
    return rowMap;
  }

  public void testByteArrayBlobDocument() throws Exception {
    byte[] blobContent = getBlobContent();
    testBLOBDataScenarios(blobContent, blobContent);
  }

  public void testSqlBlobDocument() throws Exception {
    byte[] blobContent = getBlobContent();
    long blobLength = blobContent.length;

    Blob blob = createMock(Blob.class);
    expect(blob.length()).andReturn(blobLength).anyTimes();
    expect(blob.getBytes(anyInt(), anyInt())).andReturn(blobContent).anyTimes();
    replay(blob);

    testBLOBDataScenarios(blob, blobContent);
  }

  /**
   * Test scenarios for BLOB.
   */
  public void testBLOBDataScenarios(Object blobValue, byte[] blobContent)
      throws Exception {
    Map<String, Object> rowMap = getBlobRow(blobValue);

    // Define for fetching BLOB content
    String fetchURL = "http://myhost:8030/app?dpc_id=120";
    rowMap.put(dbContext.getFetchURLField(), fetchURL);

    MimeTypeDetector.setTraversalContext(context);
    FileSizeLimitInfo fileSizeLimitInfo = new FileSizeLimitInfo();
    fileSizeLimitInfo.setMaxDocumentSize(5);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);

    JsonDocument blobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);

    // The BLOB to too large.
    assertNotNull(blobDoc);
    assertNull(Value.getSingleValue(blobDoc, SpiConstants.PROPNAME_CONTENT));

    // Increase the maximum supported size of the document.
    fileSizeLimitInfo.setMaxDocumentSize(1024 * 1024);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);
    blobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);

    assertNotNull(blobDoc);
    // Test scenario:- this doc will have column name "version" as
    // metadata key and value will be "2.3.4".
    assertEquals(rowMap.get("version"), getProperty(blobDoc, "version"));

    // Test scenario:- primary key column should be excluded while
    // indexing external metadata.
    assertNull(getProperty(blobDoc, primaryKeyColumn));

    // If one of the column holds the URL for fetching BLOB data. It
    // will be used as display URL in feed.
    assertEquals(fetchURL,
                 getProperty(blobDoc, SpiConstants.PROPNAME_DISPLAYURL));

    assertEquals("application/pdf",
        getProperty(blobDoc, SpiConstants.PROPNAME_MIMETYPE));

    assertTrue(Arrays.equals(blobContent, readBlobContent(blobDoc)));
  }

  public void testUnsupportedBlob() throws Exception {
    Map<String, Object> rowMap = getBlobRow(getBlobContent());

    // Set "application/pdf" MIME type in unsupported list. Now we should get
    // null value for DB document Content as this document is in unsupported
    // mimetype list.
    Set<String> unsupportedMime = new HashSet<String>();
    unsupportedMime.add("application/pdf");
    MimeTypeMap mimeTypeMap = new MimeTypeMap();
    mimeTypeMap.setUnsupportedMimeTypes(unsupportedMime);
    context.setMimeTypeMap(mimeTypeMap);
    JsonDocument.setTraversalContext(context);

    JsonDocument blobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
    Property docContent = blobDoc.findProperty(SpiConstants.PROPNAME_CONTENT);
    // Document content should have null value.
    assertNull(docContent);
  }

  public void testExcludedBlob() throws Exception {
    Map<String, Object> rowMap = getBlobRow(getBlobContent());

    // Set "application/pdf" MIME type in ignore list. Now we should get null
    // value for DB document as this document is ignored by connector.
    Set<String> unsupportedMime = new HashSet<String>();
    unsupportedMime.add("application/pdf");
    MimeTypeMap mimeTypeMap = new MimeTypeMap();
    mimeTypeMap.setExcludedMimeTypes(unsupportedMime);
    context.setMimeTypeMap(mimeTypeMap);
    JsonDocument.setTraversalContext(context);

    JsonDocument blobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
    try {
      blobDoc.findProperty(SpiConstants.PROPNAME_CONTENT);
      fail("Expected SkippedDocumentException, but got none.");
    } catch (SkippedDocumentException expected) {
    }
  }

  /**
   * Tests a failure in getBytes, which falls back to calling getBinaryStream.
   */
  public void testSqlBlobOneExceptionDocument() throws Exception {
    byte[] blobContent = getBlobContent();
    long blobLength = blobContent.length;
    InputStream blobStream = new ByteArrayInputStream(blobContent);

    Blob blob = createMock(Blob.class);
    expect(blob.length()).andReturn(blobLength).anyTimes();
    expect(blob.getBytes(anyInt(), anyInt()))
        .andThrow(new SQLException()).atLeastOnce();
    expect(blob.getBinaryStream()).andReturn(blobStream).atLeastOnce();
    replay(blob);

    // We can't use testBLOBDataScenarios in
    // testSqlBlobTwoExceptionsDocument, so don't use it here so we
    // know we get a non-null document without the MimeTypeDetector
    // and TraversalContext configuration.
    Map<String, Object> rowMap = getBlobRow(blob);
    JsonDocument blobDoc =
        getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
    assertNotNull(blobDoc);
    assertTrue(Arrays.equals(blobContent, readBlobContent(blobDoc)));
  }

  /**
   * Tests a failure in both getBytes and getBinaryStream, which
   * returns a null JsonDocument.
   */
  public void testSqlBlobTwoExceptionsDocument() throws Exception {
    byte[] blobContent = getBlobContent();
    long blobLength = blobContent.length;

    Blob blob = createMock(Blob.class);
    expect(blob.length()).andReturn(blobLength).anyTimes();
    expect(blob.getBytes(anyInt(), anyInt()))
        .andThrow(new SQLException()).atLeastOnce();
    expect(blob.getBinaryStream()).andThrow(new SQLException()).atLeastOnce();
    replay(blob);

    Map<String, Object> rowMap = getBlobRow(blob);
    try {
      JsonDocument blobDoc =
          getJsonDocument(new LobDocumentBuilder(dbContext, context), rowMap);
      fail("Expected DBException but got " + blobDoc.toJson());
    } catch (DBException expected) {
    }
  }

  /**
   * Test Case for fetching a BLOB File from Database and dumping it on the file
   * system using JsonDocument Object.
   */
  /* TODO: This does not work because the TESTEMPTABLE has not been set up.
   * And even if it was, it does not have any PDF BLOB data.
   */
  public void testPdfBlob(int dummy)
      throws IOException, DBException, RepositoryException {
    DBClient dbClient = dbContext.getClient();
    dbContext.setNumberOfRows(1);
    List<Map<String, Object>> rows = dbClient.executePartialQuery(0,
        dbContext.getNumberOfRows());
    JsonDocument jsonDocument = null;
    for (Map<String, Object> row : rows) {
      jsonDocument =
          getJsonDocument(new LobDocumentBuilder(dbContext, context), row);
    }

    byte[] blobcontent = readBlobContent(jsonDocument);
    assertNotNull(blobcontent);
    File newFile = new File("newreport.pdf");
    ByteStreams.write(blobcontent, Files.newOutputStreamSupplier(newFile));
  }
}
