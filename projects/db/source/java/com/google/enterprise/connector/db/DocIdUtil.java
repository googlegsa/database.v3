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

package com.google.enterprise.connector.db;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Utility class for generating, encoding, and decoding the document ID
 * generated for {@link DBDocument}. It also has other utility methods
 * for handling collections of document IDs.
 *
 * @author Suresh_Ghuge
 */
public class DocIdUtil {
  private static final Logger LOG = Logger.getLogger(DocIdUtil.class.getName());
  public static final String PRIMARY_KEYS_SEPARATOR = "/";

  /**
   * Decodes the encoded document IDs and returns the comma-separated
   * String of quoted document IDs.
   *
   * @param docIds
   * @return comma separated list of doc ids.
   */
  public static String getDocIdString(Collection<String> docIds) {
    StringBuilder docIdString = new StringBuilder("");
    for (String docId : docIds) {
      docIdString.append("'" + docId + "'" + ",");
    }
    return docIdString.substring(0, docIdString.length() - 1);
  }

  /**
   * Creates and returns a map of decoded and encoded docIds. Here
   * decoded docIds are used as keys and encoded docIds are used as values.
   *
   * @param docIds
   * @return map of decoded and encoded doc Ids
   */
  public static Map<String, String> getDocIdMap(Collection<String> docIds) {
    Map<String, String> docIdMap = new HashMap<String, String>();
    for (String docId : docIds) {
      String[] tokens = tokenizeDocId(docId);
      StringBuilder docIdString = new StringBuilder();
      // Build a legacy version of the docid, with comma-separated values.
      for (String token : tokens) {
        docIdString.append(token).append(',');
      }
      docIdString.deleteCharAt(docIdString.length() - 1);
      docIdMap.put(docIdString.toString(), docId);
    }
    return docIdMap;
  }

  /**
   * Generates the docId for a DB row.  The docid is formed from primary key
   * values separated by '/' character.  Numbers are represented as their 
   * base 10 string values.  Timestamps, Times, and Dates, are represented
   * in ISO 8601 format. All other values (including Strings) are URL encoded.
   *
   * @param primaryKeys array of primary key column names.
   * @param row map representing a row in database table.
   * @return docId encoded values of primary key columns, separated by '/'.
   * @throws DBException
   */
  public static String generateDocId(String[] primaryKeys,
      Map<String, Object> row) throws DBException {

    if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
      Set<String> keySet = row.keySet();
      StringBuilder docIdString = new StringBuilder();

      for (String primaryKey : primaryKeys) {
        /*
         * If user enters primary key column names in different case in database
         * connector config form, we need to map primary key column names
         * entered by user with actual column names in query. Below block of
         * code map the primary key column names entered by user with actual
         * column names in result set(map).
         */
        for (String key : keySet) {
          if (primaryKey.equalsIgnoreCase(key)) {
            primaryKey = key;
            break;
          }
        }
        if (!keySet.contains(primaryKey)) {
          String msg = "Primary Key does not match any of the column names.";
          LOG.warning(msg);
          throw new DBException(msg);
        }
        Object keyValue = row.get(primaryKey);
        if (null != keyValue) {
          docIdString.append(encodeValue(keyValue));
        }
        docIdString.append(PRIMARY_KEYS_SEPARATOR);
      }
      // Remove the trailing separator from the end of the docId
      docIdString.deleteCharAt(docIdString.length() - 1);

      return docIdString.toString();
    } else {
      String msg = "";
      if (row == null) {
        msg = "Database row is null";
      } else if (primaryKeys == null || primaryKeys.length == 0) {
        msg = "List of primary keys is empty or null";
      }
      LOG.warning(msg);
      throw new DBException(msg);
    }
  }

  private static final SimpleDateFormat ISO8601_DATE_FORMAT_MILLIS =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * Encode a primary key value so that it may be included in the docid.
   * Numbers are encoded as their decimal representation.  SQL Date, Time,
   * and Timestamp objects are encoded as their ISO 8601 representations,
   * as are java.util.Date objects.  All other values, including Strings,
   * are returned as URLEncoded strings.
   */
  private static String encodeValue(Object value) {
    if (value instanceof Byte) {
      return Short.toString(((Byte) value).shortValue());
    } else if (value instanceof Number) {
      // BigDecimal generates E+nn exponential notation rather than Enn.
      // I strip the '+', so that URLDecoder does not convert it to space.
      return value.toString().replaceAll("\\+","");
    } else if (value instanceof Timestamp) {
      return ((Timestamp) value).toString();
    } else if (value instanceof Time) {
      return ((Time) value).toString();
    } else if (value instanceof Date) {
      return ((Date) value).toString();
    } else if (value instanceof java.util.Date) {
      // Convert to ISO8601
      // TODO: What about timezone? I don't think it matters here.
      return ISO8601_DATE_FORMAT_MILLIS.format((java.util.Date) value);
    } else {
      try {
        return URLEncoder.encode(value.toString(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // Should not happen with UTF-8.
        throw new AssertionError(e);
      }
    } 
  }

  /**
   * Tokenizes the docid, splitting it into an array of strings.
   * Other than URLDecoding String values, no other attempt is made to
   * make sense of the tokens.
   *
   * @param docid the encoded docid string
   * @return an array of primary key values extracted from the docid string
   */
  public static String[] tokenizeDocId(String docid) {
    String[] tokens = docid.split(PRIMARY_KEYS_SEPARATOR, -1);
    try {
      for (int i = 0; i < tokens.length; i++) {
        if (tokens[i].indexOf('%') >= 0 || tokens[i].indexOf('+') >= 0) {
          tokens[i] = URLDecoder.decode(tokens[i], "UTF-8");
        }
      }
    } catch (UnsupportedEncodingException e) {
      // Should not happen with UTF-8.
      throw new AssertionError(e);
    }
    return tokens;
  }
}
