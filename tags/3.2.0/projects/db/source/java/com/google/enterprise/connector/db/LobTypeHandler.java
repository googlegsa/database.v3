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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Large Object (LOB) custom TypeHandler. The default MyBatis
 * TypeHanders return arrays of bytes or Strings. This can consume
 * considerable amounts of memory for a large number of LOBs returned
 * in a query.
 * <p/>
 * These custom handlers move the LOB content into 
 * FileBackedOutputStream-backed ContentHolders for 
 * future consumption.
 * <p/>
 * Global type handlers must be registered by the concrete Java type
 * of the column values. Since we don't know the concrete types, we
 * must register the type handlers directly by column name. We need to
 * know the column types, and we could easily get that in
 * DBConnectorType, but we don't currently have access to the column
 * types in DBClient. Instead, we register this generic type handler,
 * and use the strategy pattern to pick an implementation based on the
 * column type on the fly.
 */
/* TODO(bmj): Add NClob (and SQLXML?) support when Java 6 is required. */
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
public class LobTypeHandler extends BaseTypeHandler<DigestContentHolder> {
  private static final Logger LOGGER =
      Logger.getLogger(LobTypeHandler.class.getName());

  public interface Strategy {
    byte[] getBytes(ResultSet rs, int columnIndex) throws SQLException;

    byte[] getBytes(CallableStatement rs, int columnIndex) throws SQLException;
  }

  private static final MimeTypeDetector mimeTypeDetector =
      new MimeTypeDetector();

  private Strategy strategy = null;

  public LobTypeHandler() {
    LOGGER.config("LobTypeHandler loaded");
  }

  @Override
  public void setNonNullParameter(PreparedStatement ps, int i, 
      DigestContentHolder parameter, JdbcType jdbcType) throws SQLException {
    throw new SQLException("Unsupported Operation");
  }

  @Override
  public DigestContentHolder getNullableResult(ResultSet rs, String columnName)
      throws SQLException {
    return getNullableResult(rs, rs.findColumn(columnName));
  }

  @Override
  public DigestContentHolder getNullableResult(ResultSet rs, int columnIndex)
      throws SQLException {
    return getContentHolder(
        getStrategy(rs, columnIndex).getBytes(rs, columnIndex));
  }

  @Override
  public DigestContentHolder getNullableResult(CallableStatement cs,
      int columnIndex) throws SQLException {
    return getContentHolder(
        getStrategy(cs, columnIndex).getBytes(cs, columnIndex));
  }

  /*
   * These next methods are near duplicates, because we want to check
   * for an existing strategy first, before fetching the JDBC metadata
   * to get the column type, and because of the lack of polymorphism
   * in the JDBC API.
   */
  private synchronized Strategy getStrategy(ResultSet rs, int columnIndex)
      throws SQLException {
    if (strategy == null) {
      strategy = newStrategy(rs.getMetaData().getColumnType(columnIndex));
    }
    return strategy;
  }

  private synchronized Strategy getStrategy(CallableStatement cs,
      int columnIndex) throws SQLException {
    if (strategy == null) {
      strategy =
          newStrategy(cs.getParameterMetaData().getParameterType(columnIndex));
    }
    return strategy;
  }

  private Strategy newStrategy(int jdbcType) throws SQLException {
    Strategy value;
    switch (jdbcType) {
      case java.sql.Types.BLOB:
        value = new BlobTypeStrategy();
        break;

      case java.sql.Types.CLOB:
        value = new ClobTypeStrategy();
        break;

      case java.sql.Types.BINARY:
      case java.sql.Types.VARBINARY:
      case java.sql.Types.LONGVARBINARY:
        value = new BinaryTypeStrategy();
        break;

      default:
        // Use this for non-character types as well.
        value = new CharTypeStrategy();
        break;
    }
    LOGGER.log(Level.FINE, "Selected type strategy {0} for type {1}.",
        new Object[] { value, jdbcType });
    return value;
  }

  private DigestContentHolder getContentHolder(byte[] contentBytes)
      throws SQLException {
    DigestContentHolder contentHolder = new DigestContentHolder(
        InputStreamFactories.newInstance(contentBytes),
        mimeTypeDetector.getMimeType(null, contentBytes));
    contentHolder.updateDigest(contentBytes);
    return contentHolder;
  }
}
