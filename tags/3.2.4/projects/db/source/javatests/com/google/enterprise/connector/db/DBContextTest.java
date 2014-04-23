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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import junit.framework.TestCase;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DBContextTest extends TestCase {
  private static final Logger LOG = Logger.getLogger(DBContext.class.getName());

  private DBContext dbContext;
  private final ImmutableSet<String> columnNames =
      ImmutableSet.of("id", "firstName", "lastName", "email");
  private final ImmutableList<String> expected =
      ImmutableList.of("id", "lastName");

  @Override
  protected void setUp() throws Exception {
    dbContext = new DBContext();
  }

  public void testUppercasePrimaryKey() throws DBException {
    dbContext.setPrimaryKeys("ID,LASTNAME");
    assertEquals(expected, dbContext.getPrimaryKeyColumns(columnNames));
  }

  public void testLowercasePrimaryKey() throws DBException {
    dbContext.setPrimaryKeys("id,lastname");
    assertEquals(expected, dbContext.getPrimaryKeyColumns(columnNames));
  }

  public void testWhitespacePrimaryKey() throws DBException {
    dbContext.setPrimaryKeys("  Id , Lastname  ");
    assertEquals(expected, dbContext.getPrimaryKeyColumns(columnNames));
  }

  private void testPrimaryKeyException(String primaryKey) {
    dbContext.setPrimaryKeys(primaryKey);
    try {
      dbContext.getPrimaryKeyColumns(columnNames);
      fail("Expected a DBException");
    } catch (DBException expected) {
      LOG.log(Level.INFO, "Expected exception", expected);
    }
  }

  public void testInvalidPrimaryKey() {
    testPrimaryKeyException("invalid");
  }

  public void testEmptyPrimaryKey() {
    testPrimaryKeyException("");
  }

  public void testWhitespaceOnlyPrimaryKey() {
    testPrimaryKeyException("   ");
  }

  public void testCommaPrimaryKey() {
    testPrimaryKeyException(",");
  }

  public void testCommaWhitespacePrimaryKey() {
    testPrimaryKeyException(" ,  , ");
  }
}
