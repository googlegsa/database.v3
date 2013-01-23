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
import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.DocIdUtil;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents both flavors of snapshot, from a snapshot file and from
 * a {@code SnapshotRepository}.
 */
public class DBSnapshot
    implements DocumentSnapshot, Comparable<DocumentSnapshot> {

  private static final Logger LOG =
      Logger.getLogger(DBSnapshot.class.getName());

  /** An optional document holder, may be null. */
  private final DocumentBuilder.DocumentHolder docHolder;
  private final String documentId;
  private final String jsonString;

  /** Constructs a snapshot from a {@code DBSnapshotRepository}. */
  public DBSnapshot(String documentId, String jsonString,
      DocumentBuilder.DocumentHolder docHolder) {
    this.docHolder = docHolder;
    this.documentId = documentId;
    this.jsonString = jsonString;
  }

  /** Reconstructs a snapshot from a snapshot file. */
  public DBSnapshot(String jsonString) {
    this.docHolder = null;
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
   * Compares this {@link DBSnapshot} to the referenced document on the GSA
   * to detect inserted or deleted records.  If, by returning 0, this snapshot
   * represents the same record as the supplied paramenter, then a subsequent
   * call to {@link #getUpdate(DocumentSnapshot)} may be made to determine
   * whether the record changed.
   *
   * @return a negative integer, zero, or a positive integer as this snapshot
   *         is less than, equal to, or greater than the specified snapshot.
   * @throws NullPointerException if the specified snapshot is null.
   */
  @Override
  public int compareTo(DocumentSnapshot onGsa) throws ClassCastException {
    String otherDocid = onGsa.getDocumentId();
    // If the docid strings are identical, then these are the same record.
    if (documentId.equals(otherDocid)) {
      return 0;
    }

    // Tokenize the docids, and compare the individual primary key values.
    String[] myValues = DocIdUtil.tokenizeDocId(documentId);
    String[] otherValues = DocIdUtil.tokenizeDocId(otherDocid);

    int i;
    for (i = 0; i < myValues.length; i++) {
      // Old-style Base64-encoded docids will look too short.
      if (i >= otherValues.length) {
        return 1;
      }
      String myValue = myValues[i];
      String otherValue = otherValues[i];
      int rtn = compareAsNumbers(myValue, otherValue);
      // Using a return of MAX_VALUE to signal that these did not compare
      // as numbers, so try comparing as strings.
      // TODO (bmj): This will fail on SQL String values that both look like
      // numbers.
      if (rtn == Integer.MAX_VALUE) {
        rtn = myValue.compareTo(otherValue);
      }
      if (rtn != 0) {
        return rtn;
      }
    }
    return i - otherValues.length;
  }

  // Pattern for all manner of integers (shorts, ints, longs, bigints, etc).
  private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?[0-9]+$");
  // Pattern for all manner of real numbers (floats, doubles, big decimal, etc).
  private static final Pattern REAL_PATTERN =
      Pattern.compile("^-?[0-9]*\\.?[0-9]+(E-?[0-9]+)?$");

  /**
   * Compare the two values as if they were numbers.
   *
   * @return Integer.MAX_VALUE if values are not numbers.
   *   -1 if values were numbers and val1 is less than val2.
   *    0 if values were numbers and are equal. 
   *    1 if values were numbers and val1 is greater than val2.
   */
  private static int compareAsNumbers(String val1, String val2) {
    if (INTEGER_PATTERN.matcher(val1).matches() && 
        INTEGER_PATTERN.matcher(val2).matches()) {
      try {
        // First try them as longs.
        return Long.signum(Long.parseLong(val1) - Long.parseLong(val2));
      } catch (NumberFormatException e) {
        // Then try BigInteger.
        try {
          return new BigInteger(val1).compareTo(new BigInteger(val2));
        } catch (NumberFormatException e1) {
          return Integer.MAX_VALUE;          
        }
      }
    } else if (REAL_PATTERN.matcher(val1).matches() && 
        REAL_PATTERN.matcher(val2).matches()) {
      try {
        // First try them as doubles.
        return Double.compare(Double.parseDouble(val1),
                              Double.parseDouble(val2));
      } catch (NumberFormatException e) {
        // Then try BigDecimal.
        try {
          return new BigDecimal(val1).compareTo(new BigDecimal(val2));
        } catch (NumberFormatException e1) {
          return Integer.MAX_VALUE;          
        }
      }
    }
    return Integer.MAX_VALUE;
  }

  /**
   * Returns a {@link DocumentHandle} for updating the referenced document on
   * the GSA or null if the document on the GSA does not need updating.
   */
  @Override
  public DocumentHandle getUpdate(DocumentSnapshot onGsa)
      throws RepositoryException {
    if (docHolder == null) {
      throw new UnsupportedOperationException(
          "getUpdate called on deserialized snapshot.");
    }

    // The diffing framework sends in a null to indicate that it has not seen
    // this snapshot before. So we return the corresponding handle.
    if (onGsa == null) {
      return getDocumentHandle();
    }

    // If the parameter is non-null, then it should be an DBSnapshot
    // (it was created via a DBSnapshotFactory).
    if (!(onGsa instanceof DBSnapshot)) {
      throw new IllegalArgumentException(
          "Illegal parameter passed to getUpdate. "
          + "The parameter passed is not an instance of DBSnapshot.");
    }

    // We just assume that if the serialized form is the same, then nothing
    // has changed.
    if (jsonString.equals(onGsa.toString())) {
      // null return tells the diffing framework to do nothing
      return null;
    }

    // Something has changed, so return the corresponding handle
    // and set the changed flag of the document.
    LOG.info("Change for Document with Id " + getDocumentId() + " at time "
        + new Date());
    return getDocumentHandle();
  }

  private DocumentHandle getDocumentHandle() {
    try {
      return docHolder.getDocumentHandle();
    } catch (DBException e) {
      // If we can't get the document handle, return null to indicate no
      // change. The most likely source of errors is the XSLT transform.
      // See the similar log message in RepositoryHandler.getDocList.
      LOG.warning("Cannot convert database record to snapshot for "
          + "record " + getDocumentId() + "\n" + e);
      return null;
    }
  }

  @Override
  public String toString() {
    return jsonString;
  }
}
