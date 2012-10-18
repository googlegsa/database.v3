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

import com.google.enterprise.connector.util.Base64;
import com.google.enterprise.connector.util.Base64DecoderException;

import java.io.IOException;
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
  public static final String PRIMARY_KEYS_SEPARATOR = ",";

  /**
   * Decodes the Base64-encoded document IDs and returns the comma-separated
   * String of document IDs.
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
      docIdMap.put(decodeBase64String(docId), docId);
    }
    return docIdMap;
  }

  /**
   * Decodes the Base64-encoded input string.
   *
   * @param inputString
   * @return BASE64 decoded string
   * @throws IOException
   */
  public static String decodeBase64String(String inputString) {
    byte[] docId;
    try {
      docId = Base64.decode(inputString.getBytes());
    } catch (Base64DecoderException e) {
      LOG.warning("Exception thrown while decoding docId: " + inputString
          + "\n" + e);
      return null;
    }
    return new String(docId);
  }

  public static String getBase64EncodedString(String inputString) {
    return Base64.encodeWebSafe(inputString.getBytes(), false);
  }

  /**
   * Generates the docId for a DB row. Base64 encode comma separated key values
   * are used as document ID. For example, if the primary keys are ID and
   * lastName and their corresponding values are 1 and last_01, then the docId
   * would be the Base64-encoded of "1,last_01" i.e "MSxmaXJzdF8wMQ==".
   *
   * @param primaryKeys : array of primary key columns
   * @param row : map representing a row in database table.
   * @return docId :- Base 64 encoded values of comma separated primary key
   *         columns.
   * @throws DBException
   */
  public static String generateDocId(String[] primaryKeys,
      Map<String, Object> row) throws DBException {

    StringBuilder docIdString = new StringBuilder();
    if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
      Set<String> keySet = row.keySet();

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
          String msg = "Primary Key does not match with any of the coulmn names";
          LOG.warning(msg);
          throw new DBException(msg);
        }
        Object keyValue = row.get(primaryKey);
        if (null != keyValue) {
          docIdString.append(keyValue.toString() + PRIMARY_KEYS_SEPARATOR);
        }
      }
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

    // Remove the extra "," from the end of the docId, if present.
    char lastChar = docIdString.charAt(docIdString.length() - 1);
    if (lastChar == ',') {
      docIdString.deleteCharAt(docIdString.length() - 1);
    }
    // Encode doc ID.
    String encodedDocId = getBase64EncodedString(docIdString.toString());
    return encodedDocId;
  }
}
