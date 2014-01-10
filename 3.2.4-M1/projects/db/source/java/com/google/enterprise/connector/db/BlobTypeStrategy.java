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

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlobTypeStrategy implements LobTypeHandler.Strategy {
  private static final Logger LOGGER =
      Logger.getLogger(BlobTypeStrategy.class.getName());

  @Override
  public byte[] getBytes(ResultSet rs, int columnIndex) throws SQLException {
    return getBytes(rs.getBlob(columnIndex));
  }

  @Override
  public byte[] getBytes(CallableStatement cs, int columnIndex)
      throws SQLException {
    return getBytes(cs.getBlob(columnIndex));
  }

  private byte[] getBytes(Blob blob) throws SQLException {
    if (blob == null) {
      LOGGER.log(Level.FINEST, "BLOB handler called with null BLOB");
      return new byte[0];
    }

    LOGGER.log(Level.FINEST, "BLOB handler called with BLOB of length {0}",
        blob.length());
    byte[] bytes = blob.getBytes(1, (int) blob.length());
    try {
      blob.free();
    } catch (SQLException e) {
      LOGGER.log(Level.WARNING, "Error freeing the BLOB", e);
    } catch (UnsupportedOperationException e) {
      // Check for JDBC drivers that don't support this JDBC 4.0 method.
      // This is an unusual exception to throw, but worth catching since
      // it's a RuntimeException that will otherwise terminate the monitor.
      LOGGER.log(Level.WARNING,
          "Error freeing the BLOB, try a newer JDBC 4.0 driver", e);
    } catch (LinkageError e) {
      // Check for JDBC drivers that were not compiled against Java 6.
      LOGGER.log(Level.WARNING,
          "Error freeing the BLOB, try a newer JDBC 4.0 driver", e);
    }
    return bytes;
  }
}
