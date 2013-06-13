// Copyright 2013 Google Inc.
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

import com.google.enterprise.connector.db.diffing.DigestContentHolder;
import com.google.enterprise.connector.util.InputStreamFactory;
import com.google.enterprise.connector.util.MimeTypeDetector;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An abstract Large Object (LOB) custom TypeHandler.
 * The default MyBatis TypeHanders for BLOB and CLOB
 * fields return arrays of bytes or Strings, respectively.
 * This can consume considerable amounts of memory for 
 * a large number of LOBs returned in a query.
 * <p/>
 * These custom handlers move the LOB content into 
 * FileBackedOutputStream-backed ContentHolders for 
 * future consumption.
 * <p/>
 * Subclasses implement support for BLOB or CLOB objects.
 */
/* TODO(bmj): Add NClob subclass when Java 6 is required. */
/* TODO(bmj): getBytes() could end up allocating significant amounts of memory
 * if the LOB field is large.  We should create the FileBackedOutputStream
 * here and shuffle content from the LOB InputStream (or CharacterStream)
 * into the FileBackedOutputStream.  We should also do the mime type detection
 * and checksum calculation on the bytes as we read them.  Finally, add a
 * InputStreamFactories.newInstance(FileBackedOutputStream s, long length)
 * method that wraps a FileBackedInputStreamFactory around the existing
 * FileBackedOutputStream.
 */
/* TODO(bmj): Get access to the TraversalContext so we can skip mime types
 * we don't support, or supply zero-length content for LOBs that exceed
 * the max document size.
 */
public abstract class LobTypeHandler<T>  // T is Blob, Clob, or NClob
    extends BaseTypeHandler<DigestContentHolder> {

  private static final Logger LOGGER =
      Logger.getLogger(BlobTypeHandler.class.getName());

  private static final MimeTypeDetector mimeTypeDetector =
      new MimeTypeDetector();

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, 
      DigestContentHolder parameter, JdbcType jdbcType) throws SQLException {
    throw new SQLException("Unsupported Operation");
  }

  /** @return an array of bytes containing the LOB content. */
  protected abstract byte[] getBytes(T lob) throws SQLException;

  /** Does nothing, because Blob.free() and Clob.free() are Java 6 only. */
  /* TODO(bmj): Subclasses should override this when Java 6 is required. */
  protected void free(T lob) throws SQLException {
  }

  protected DigestContentHolder getContentHolder(T lob) throws SQLException {
    byte[] contentBytes = getBytes(lob);
    DigestContentHolder contentHolder = new DigestContentHolder(
        InputStreamFactories.newInstance(contentBytes),
        mimeTypeDetector.getMimeType(null, contentBytes));
    contentHolder.updateDigest(contentBytes);
    
    try {
       free(lob);
    } catch (SQLException e) {
      LOGGER.log(Level.FINEST, "Failed to free LOB", e);
    }
    return contentHolder;
  }
}
