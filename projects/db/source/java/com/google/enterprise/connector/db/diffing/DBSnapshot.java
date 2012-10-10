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

import com.google.common.base.Function;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents both flavors of snapshot, from a snapshot file and from
 * a {@code SnapshotRepository}.
 */
public class DBSnapshot implements DocumentSnapshot {
  private static final Logger LOG =
      Logger.getLogger(DBSnapshot.class.getName());

  /** An optional document, may be null. */
  private final JsonDocument document;
  private final String documentId;
  private final String jsonString;

  /** Constructs a snapshot from a {@code DBSnapshotRepository}. */
  public DBSnapshot(JsonDocument jsonDoc) {
    document = jsonDoc;
    documentId = document.getDocumentId();
    jsonString = document.toJson();
  }

  public static Function<JsonDocument, DBSnapshot> factoryFunction =
      new Function<JsonDocument, DBSnapshot>() {
    @Override
    public DBSnapshot apply(JsonDocument jsonDoc) {
      DBSnapshot p = new DBSnapshot(jsonDoc);
      if (LOG.isLoggable(Level.FINER)) {
        LOG.finer("DBSnapshotRepository returns document with docID "
            + DocIdUtil.decodeBase64String(p.getDocumentId().toString()));
      }
      return p;
    }
  };

  /** Reconstructs a snapshot from a snapshot file. */
  public DBSnapshot(String jsonString) {
    this.document = null;
    try {
      JSONObject jo = new JSONObject(jsonString);
      this.documentId = jo.getString(SpiConstants.PROPNAME_DOCID);
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, "Invalid serialized snapshot: " + jsonString, e);
      throw new IllegalArgumentException(
          "Invalid serialized snapshot: " + jsonString, e);
    }
    this.jsonString = jsonString;
  }

  @Override
  public String getDocumentId() {
    return documentId;
  }

  /**
   * Returns a {@link DocumentHandle} for updating the referenced document on
   * the GSA or null if the document on the GSA does not need updating.
   */
  @Override
  public DocumentHandle getUpdate(DocumentSnapshot onGsa)
      throws RepositoryException {
    if (document == null) {
      throw new UnsupportedOperationException(
          "getUpdate called on deserialized snapshot.");
    }

    // The diffing framework sends in a null to indicate that it has not seen
    // this snapshot before. So we return the corresponding handle.
    if (onGsa == null) {
      this.document.setChanged();
      return new DBHandle(document);
    }

    // If the parameter is non-null, then it should be an DBSnapshot
    // (it was created via a DBSnapshotFactory).
    if (!(onGsa instanceof DBSnapshot)) {
      throw new IllegalArgumentException(
          "Illegal parameter passed to getUpdate. "
          + "The paramater passed is not an instance of DBSnapshot");
    }

    // We just assume that if the serialized form is the same, then nothing
    // has changed.
    if (jsonString.equals(onGsa.toString())) {
      // null return tells the diffing framework to do nothing
      return null;
    }

    // Something has changed, so return the corresponding handle
    // and set the changed flag of the document.
    this.document.setChanged();
    LOG.info("Change for Document with Id " + getDocumentId() + " at time "
        + new Date());
    return new DBHandle(document);
  }

  @Override
  public String toString() {
    return jsonString;
  }
}
