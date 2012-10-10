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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.enterprise.connector.db.diffing.JsonDocument;
import com.google.enterprise.connector.db.diffing.JsonDocumentUtil;
import com.google.enterprise.connector.db.diffing.LobDocumentBuilder;
import com.google.enterprise.connector.db.diffing.MetadataDocumentBuilder;
import com.google.enterprise.connector.db.diffing.UrlDocumentBuilder;
import com.google.enterprise.connector.db.testing.MockClient;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.spiimpl.BinaryValue;
import com.google.enterprise.connector.traversal.FileSizeLimitInfo;
import com.google.enterprise.connector.traversal.MimeTypeMap;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;
import com.google.enterprise.connector.util.MimeTypeDetector;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

/*
 * TODO: Split this into {Lob,Url,Metadata}DocumentBuilderTest classes
 * in the diffing package when fewer changes are in-flight.
 */
public class JsonDocumentUtilTest extends DBTestBase {
  private static final Logger LOG =
      Logger.getLogger(JsonDocumentUtilTest.class.getName());

  private final ProductionTraversalContext context =
      new ProductionTraversalContext();
  private final String primaryKeyColumn = "id";

  private DBContext dbContext;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    LOG.info("Test " + getName());

    dbContext = super.getDbContext();
    dbContext.setPrimaryKeys(primaryKeyColumn);
    dbContext.setHostname("localhost");
  }

  /**
   * Test for converting DB row to DB Doc.
   */
  public final void testRowToDoc() throws Exception {
    Map<String, Object> rowMap = TestUtils.getStandardDBRow();
    JsonDocument doc =
        new MetadataDocumentBuilder(getMinimalDbContext()).fromRow(rowMap);
    for (String propName : doc.getPropertyNames()) {
      LOG.info(propName + ":    " + getProperty(doc, propName));
    }
    assertEquals("MSxsYXN0XzAx", getProperty(doc, SpiConstants.PROPNAME_DOCID));
    String content = getProperty(doc, SpiConstants.PROPNAME_CONTENT);
    assertNotNull(content);
    assertTrue(content.contains("id=1"));
    assertTrue(content.contains("lastName=last_01"));
    assertEquals("text/html", getProperty(doc, SpiConstants.PROPNAME_MIMETYPE));

    // Checksum should be hidden as a public property.
    assertNull(doc.findProperty(JsonDocumentUtil.ROW_CHECKSUM));
      
    // But the checksum should be included in the snapshot string.
    String expected = "{\"google:docid\":\"MSxsYXN0XzAx\","
        + "\"google:sum\":\"7ffd1d7efaf0d1ee260c646d827020651519e7b0\"}";
    assertEquals(expected, doc.toJson());
  }

  /**
   * Test case for generateURLMetaFeed().
   */
  public final void testUrlDocument() throws Exception {
    Map<String, Object> rowMap = new HashMap<String, Object>();

    String documentURL = "http://myhost/app/welcome.html";
    String versionColumn = "version";
    String versionValue = "2.3.4";

    // add primary key in row
    rowMap.put(primaryKeyColumn, 1);
    // add document URL in row
    rowMap.put(dbContext.getDocumentURLField(), documentURL);
    // add version column in row
    rowMap.put(versionColumn, versionValue);

    JsonDocument doc = new UrlDocumentBuilder(dbContext, "").fromRow(rowMap);
    // Test:- column "version" value as metadata.
    assertEquals(versionValue, getProperty(doc, versionColumn));

    // Test:- display URL will be same as the actual URL of the document.
    assertEquals("http://myhost/app/welcome.html", getProperty(doc, 
        SpiConstants.PROPNAME_DISPLAYURL));
  }

  public final void testDocIdDocument() throws Exception {
    Map<String, Object> rowMapWithBaseURL = new HashMap<String, Object>();

    String baseURL = "http://myhost/app/";
    String docId = "index123.html";
    String versionColumn = "version";
    String versionValue = "2.3.4";

    // Test scenario: when base URL is not empty, the display URL is
    // generated by concatenating document id with base URL.
    rowMapWithBaseURL.put(primaryKeyColumn, 2);
    rowMapWithBaseURL.put(dbContext.getDocumentIdField(), docId);
    rowMapWithBaseURL.put(versionColumn, versionValue);

    JsonDocument docWithBaseURL =
        new UrlDocumentBuilder(dbContext, JsonDocumentUtil.WITH_BASE_URL)
        .fromRow(rowMapWithBaseURL);

    // Test:- column "version" value as metadata.
    assertEquals(versionValue, getProperty(docWithBaseURL, versionColumn));

    // Test: display URL of the document.
    assertEquals(baseURL + docId, getProperty(docWithBaseURL, 
        SpiConstants.PROPNAME_DISPLAYURL));
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

  /* TODO: Move this to MockClient and generalize it like MockClient.getBlob. */
  private String getClobContent() {
    // Define CLOB data larger than the FileBackedOutputStream will
    // hold in memory for this test case.
    StringBuilder builder = new StringBuilder(100000);
    Random random = new Random();
    for (int i = 0; i < 100000; i++) {
      builder.append(Character.toChars(32 + random.nextInt(94)));
    }
    return builder.toString();
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
        new LobDocumentBuilder(dbContext, context).fromRow(rowMap);
    // As the size of the document is more than supported, clobDoc should have
    // null value.
    assertNull(clobDoc);
    // Increase the maximum supported size of the document.
    fileSizeLimitInfo.setMaxDocumentSize(1024 * 1024);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);
    clobDoc = new LobDocumentBuilder(dbContext, context).fromRow(rowMap);
    assertNotNull(clobDoc);

    // Test scenario:- this doc will have column name "version" as
    // metadata key and value will be "2.3.4".
    assertEquals(rowMap.get("version"), getProperty(clobDoc, "version"));

    // Test scenario:- the content of this document will be same as the
    // content of CLOB column.
    String actualContent = new String(readBlobContent(clobDoc), "UTF-8");
    assertEquals(clobContent, actualContent);

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
    InputStream blobStream = new ByteArrayInputStream(blobContent);

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
    fileSizeLimitInfo.setMaxDocumentSize(1024);
    context.setFileSizeLimitInfo(fileSizeLimitInfo);

    JsonDocument blobDoc =
        new LobDocumentBuilder(dbContext, context).fromRow(rowMap);

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
        new LobDocumentBuilder(dbContext, context).fromRow(rowMap);
    blobDoc.setChanged();
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
        new LobDocumentBuilder(dbContext, context).fromRow(rowMap);
    blobDoc.setChanged();
    try {
      blobDoc.findProperty(SpiConstants.PROPNAME_CONTENT);
      fail("Expected SkippedDocumentException, but got none.");
    } catch (SkippedDocumentException e) {
      LOG.info("Skipped Document Exception thrown for ignored mimetype");
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
      // TODO: This used to use "mysql" as the dbName, but now it gets
      // "testdb_" from the dbContext. Fix that or just remove this TODO.
      jsonDocument = new LobDocumentBuilder(dbContext, context).fromRow(row);
    }
    jsonDocument.setChanged();

    byte[] blobcontent = readBlobContent(jsonDocument);
    File newFile = new File("newreport.pdf");
    ByteStreams.write(blobcontent, Files.newOutputStreamSupplier(newFile));
  }

  private byte[] readBlobContent(JsonDocument doc)
      throws IOException, RepositoryException {
    Value value = Value.getSingleValue(doc, SpiConstants.PROPNAME_CONTENT);
    assertNotNull(value);
    InputStream is = ((BinaryValue) value).getInputStream();
    byte[] blobContent = ByteStreams.toByteArray(is);
    is.close();
    return blobContent;
  }    

  private String getProperty(JsonDocument doc, String propName) 
      throws RepositoryException {
    return Value.getSingleValueString(doc, propName);
  }
}
