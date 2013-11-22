// Copyright 2012 Google Inc.
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

package com.google.enterprise.connector.db.testing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.enterprise.connector.db.DBClient;
import com.google.enterprise.connector.db.DBContext;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

/**
 * A stub client that does not rely on live data in a database and
 * that returns a configurable sequence of batches.
 */
public class MockClient extends DBClient {
  private static final Logger LOG =
      Logger.getLogger(MockClient.class.getName());

  // TODO: String.getBytes(Charsets.UTF_8) requires Java 6.
  private static final byte[] PDF_PREFIX;

  static {
    try {
      // TODO: We could work harder to make this valid PDF.
      // Include two consecutive nulls to force MimeTypeDetector to
      // think it's binary rather than text/plain.
      PDF_PREFIX = "%PDF-1.3\n%\0\0\n".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Generates a byte array of a given size, filled with either zeroes
   * or random bytes, prefixed with %PDF- to get a valid media type
   * from the detector.
   */
  public static byte[] getBlob(int size, boolean random) {
    // Set a minimum size to get reasonable randomness: 256^4 = 2^32
    // permutations.
    byte[] blob = new byte[Math.max(size, PDF_PREFIX.length + 4)];
    if (random) {
      new Random().nextBytes(blob);
    }

    System.arraycopy(PDF_PREFIX, 0, blob, 0, PDF_PREFIX.length);

    return blob;
  }

  /**
   * Generates a String of a given size, filled with random ASCII
   * printable characters.
   */
  public static String getClob(int size) {
    // TODO: Support configurable randomness.
    // TODO: Add spaces to the string.
    StringBuilder builder = new StringBuilder(size);
    Random random = new Random();
    for (int i = 0; i < size; i++) {
      builder.append(Character.toChars(32 + random.nextInt(94)));
    }
    return builder.toString();
  }

  /** The number of rows to return altogether. */
  private int rowCount = 1000;

  /** The size of the returned BLOBs. */
  private int blobSize = 100000;

  /**
   * Use a single BLOB value for all rows (the default, to conserve
   * memory and test other things), or create a new one for each row
   * (to simulate real memory usage).
   */
  private boolean blobSingleton = true;

  /** Generate a BLOB value with random bytes or all zeroes (the default). */
  // TODO: Setting this to true forces the connector to feed every row
  // on every pass. The intent was only to create a more realistic
  // feed size, because the zero-filled BLOB compresses way too much.
  // We could add another parameter to create stable values between
  // passes even when using random values for each row. We have to
  // avoid caching the entire random BLOBs (too much memory use), but
  // maybe we could store a random seed for each of the rowCount rows.
  private boolean blobRandom = false;

  /** The singleton or current BLOB value. The current value is not used. */
  private byte[] blob;

  public MockClient() {
  }

  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
  }

  public void setBlobSize(int blobSize) {
    this.blobSize = blobSize;
  }

  public void setBlobSingleton(boolean blobSingleton) {
    this.blobSingleton = blobSingleton;
  }

  public void setBlobRandom(boolean blobRandom) {
    this.blobRandom = blobRandom;
  }

  private synchronized byte[] getBlob() {
    if (blobSingleton && blob == null || !blobSingleton) {
      blob = getBlob(blobSize, blobRandom);
    }
    return blob;
  }

  @Override
  public List<Map<String, Object>> executePartialQuery(int skipRows,
      int maxRows) {
    LOG.info("Executing mock query with skipRows = " + skipRows + " and "
        + "maxRows = " + maxRows);

    // TODO(jlacey): Use the configuration in DBContext along with
    // executing the SQL query to determine the columns to return here.
    List<Map<String, Object>> rows = Lists.newArrayList();
    for (int i = skipRows; i < rowCount && i - skipRows < maxRows; i++) {
      Map<String, Object> row = Maps.newHashMap();
      row.put("ID", Integer.toString(i));
      row.put("NAME", "name_" + i);
      row.put("BIG", getBlob());
      rows.add(row);
    }

    LOG.info("Number of rows returned " + rows.size());
    return rows;
  }
}
