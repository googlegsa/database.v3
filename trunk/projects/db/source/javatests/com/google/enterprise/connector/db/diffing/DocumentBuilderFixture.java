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

import com.google.enterprise.connector.db.DBContext;
import com.google.enterprise.connector.db.DBTestBase;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.traversal.ProductionTraversalContext;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.util.logging.Logger;

public abstract class DocumentBuilderFixture extends DBTestBase {
  private static final Logger LOG =
      Logger.getLogger(DocumentBuilderFixture.class.getName());

  protected final ProductionTraversalContext context =
      new ProductionTraversalContext();
  protected final String primaryKeyColumn = "id";

  protected DBContext dbContext;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    LOG.info("Test " + getName());

    dbContext = super.getDbContext();
    dbContext.setPrimaryKeys(primaryKeyColumn);
    dbContext.setHostname("localhost");
  }

  protected String getProperty(JsonDocument doc, String propName)
      throws RepositoryException {
    return Value.getSingleValueString(doc, propName);
  }
}
