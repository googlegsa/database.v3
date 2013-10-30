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

import com.google.common.base.Charsets;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CharTypeStrategy implements LobTypeHandler.Strategy {
  private static final Logger LOGGER =
      Logger.getLogger(CharTypeStrategy.class.getName());

  @Override
  public byte[] getBytes(ResultSet rs, int columnIndex) throws SQLException {
    return getBytes(rs.getString(columnIndex));
  }

  @Override
  public byte[] getBytes(CallableStatement cs, int columnIndex)
      throws SQLException {
    return getBytes(cs.getString(columnIndex));
  }

  private byte[] getBytes(String value) throws SQLException {
    LOGGER.log(Level.FINEST,
        "LONGVARCHAR handler called with string of length {0}", value.length());
    return value.getBytes(Charsets.UTF_8);
  }
}
