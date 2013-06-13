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

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.diffing.DocumentHandle;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Logger;

/**
 * Backed with a {@link JsonDocument}.
 */
public class DBHandle implements DocumentHandle {
  private static final Logger LOG = Logger.getLogger(DBHandle.class.getName());

  private final JsonDocument document;
  private final String documentId;

  /** Constructs a {@code DocumentHandle} wrapper on a {@code Document}. */
  public DBHandle(JsonDocument jsonDoc) {
    document = jsonDoc;
    documentId = document.getDocumentId();
  }

  /**
   * Reconstructs a {@code DocumentHandle} from a serialized string
   * representation.
   */
  public DBHandle(String jsonString) {
    JSONObject jo;
    try {
      jo = new JSONObject(jsonString);
    } catch (JSONException e) {
      LOG.warning("Exception thrown while creating JSONObject from string"
          + jsonString + "\n" + e.toString());
      throw new IllegalArgumentException(
          "Exception thrown for illegal JsonString" + jsonString + "\n", e);
    }
    document = new JsonDocument(jo);
    try {
      documentId = Value.getSingleValueString(document,
                                              SpiConstants.PROPNAME_DOCID);
    } catch (RepositoryException e) {
      LOG.warning("Exception thrown while extracting docId for Document"
          + document + "\n" + e.toString());
      // Thrown to indicate an inappropriate argument has been passed to
      // Value.getSingleValueString() method.
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Document getDocument() throws RepositoryException {
    return document;
  }

  @Override
  public String getDocumentId() {
    return documentId;
  }

  @Override
  public String toString() {
    return document.toJson();
  }
}
