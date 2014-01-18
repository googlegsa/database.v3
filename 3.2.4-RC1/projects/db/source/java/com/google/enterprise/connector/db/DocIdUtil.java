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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
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
   * We will encode the various primary key value types into the docid so that
   * we can reconstruct those values. The types encoding is pretty brain-dead,
   * using the uppercase letters of the ASCII alphabet, based on the 
   * ordinal value for each type (0 == 'A', 1 == 'B', etc).
   */
  static enum Type {
    // WARNING: Only add new types at end, as their ordinal values are
    // persisted in the docids.
    NULL(false), LONG(true), DOUBLE(true), BIGINT(true), BIGDEC(true),
    STRING(false), UTILDATE(false), TIMESTAMP(false), DATE(false),
    TIME(false), BOOL(false);

    private static Type[] types = Type.values();
    private final boolean isNumeric;

    Type(boolean isNumeric) {
      this.isNumeric = isNumeric;
    }

    /** Returns true if this is a numeric type, false otherwise. */
    public boolean isNumeric() {
      return isNumeric;
    }

    /** Returns the single character code for this type. */
    public char typeCode() {
      return (char) ('A' + ordinal());
    }

    /** Returns the enum that matches the supplied single character code. */
    public static Type valueOf(char code) {
      if (code < 'A' || code > 'A' + types.length) {
        throw new IllegalArgumentException("Type code must be in the range A-"
                                           +  (char) ('A' + types.length));
      }
      return types[code - 'A'];
    }
  }

  /**
   * Decodes the encoded document IDs and returns the comma-separated
   * String of quoted document IDs.
   *
   * @param docIds
   * @return comma separated list of doc ids.
   */
  public static String getDocIdString(Collection<String> docIds) {
    StringBuilder docIdString = new StringBuilder("");
    // TODO(bmj): This should be fixed for numeric primary keys.
    // Enclosing them in quotes is a SQL syntax error.
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
      String[] tokens = docId.split(PRIMARY_KEYS_SEPARATOR, -1);
      if (tokens.length == 1) {
        // Legacy Base64-encoded docid.
        try {
          docIdMap.put(new String(Base64.decode(docId)), docId);
        } catch (Base64DecoderException e) {
          LOG.log(Level.WARNING, "Error decoding docId: " + docId, e);
        }
      } else {
        String types = tokens[0];
        StringBuilder docIdString = new StringBuilder();
        // Build a legacy version of the docid, with comma-separated values.
        // TODO(bmj): This is fundmentally broken for multi-valued primary keys.
        for (int i = 1; i < tokens.length; i++) {
          if (types.charAt(i - 1) == Type.STRING.typeCode()) {
            docIdString.append(urlDecode(tokens[i]));
          } else {
            docIdString.append(tokens[i]);
          }
          docIdString.append(',');
        }
        docIdString.deleteCharAt(docIdString.length() - 1);
        docIdMap.put(docIdString.toString(), docId);
      }
    }
    return docIdMap;
  }

  /**
   * Generates the docId for a DB row.  The docid is formed from primary key
   * values separated by '/' character, preceded by a string that identifies
   * the types for each of the values.  Numbers are represented as their 
   * base 10 string values.  Timestamps, Times, and Dates, are represented
   * in ISO 8601 format. All other values (including Strings) are URL encoded.
   * </p>
   * For instance, if a compound primary key consists of an integer and a
   * string, and a particular record's primary keys are 10 and 'hello world',
   * then its docid would be "BE/10/hello+world".  The types string 'BE'
   * indicates that the value '10' is a long integer and that 'hello+world'
   * is a string.
   * </p>
   * The use of '/' separators and human-readable values provide document
   * URLs that could leverage certain features of the GSA.  Encoding the types
   * into the docid allows the individual values to be extracted and treated
   * appropriately.  For instance when ordering docids, docids consisting of
   * numeric primary keys are sorted numerically, rather than lexigraphically.
   *
   * @param primaryKeys array of primary key column names.
   * @param row map representing a row in database table.
   * @return docId encoded values of primary key columns, separated by '/'.
   * @throws DBException
   */
  public static String generateDocId(List<String> primaryKeys,
      Map<String, Object> row) {
    StringBuilder values = new StringBuilder();
    StringBuilder types = new StringBuilder();
    for (String primaryKey : primaryKeys) {
      values.append(PRIMARY_KEYS_SEPARATOR);
      appendValue(row.get(primaryKey), values, types);
    }
    // Now glue the types and values together as the docId.
    return types.toString() + values.toString();
  }

  private static final SimpleDateFormat ISO8601_DATE_FORMAT_MILLIS =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * Encode a primary key value so that it may be included in the docid.
   * Numbers are encoded as their decimal representation.  SQL Date, Time,
   * and Timestamp objects are encoded as their ISO 8601 representations,
   * as are java.util.Date objects.  All other values, including Strings,
   * are returned as URLEncoded strings.
   *
   * @param value the primary key value to encode
   * @param values the values encoding under construction
   * @param types the types encoding under construction
   */
  private static void appendValue(Object value, StringBuilder values,
                                  StringBuilder types) {
    String valueStr;
    Type type;
    if (value == null) {
      valueStr = "";
      type = Type.NULL;
    } else if (value instanceof Number) {
      if (value instanceof BigDecimal) {
        // BigDecimal generates E+nn exponential notation rather than Enn.
        // I strip the '+', so that URLDecoder does not convert it to space.
        valueStr = value.toString().replaceAll("\\+", "");
        type = Type.BIGDEC;
      } else if (value instanceof BigInteger) {
        valueStr = value.toString();
        type = Type.BIGINT;
      } else if (value instanceof Float || value instanceof Double) {
        valueStr = value.toString();
        type = Type.DOUBLE;
      } else {
        valueStr = value.toString();
        type = Type.LONG;
      }
    } else if (value instanceof java.util.Date) {
      if (value instanceof Timestamp) {
        valueStr = value.toString();
        type = Type.TIMESTAMP;
      } else if (value instanceof Time) {
        valueStr = value.toString();
        type = Type.TIME;
      } else if (value instanceof Date) {
        valueStr = value.toString();
        type = Type.DATE;
      } else {
        // Convert to ISO8601
        // TODO: What about timezone? I don't think it matters here.
        valueStr = ISO8601_DATE_FORMAT_MILLIS.format((java.util.Date) value);
        type = Type.UTILDATE;
      }
    } else if (value instanceof Boolean) {
      valueStr = value.toString();
      type = Type.BOOL;
    } else {
      // All other types (including Strings) are URLencoded strings.
      valueStr = urlEncode(value.toString());
      type = Type.STRING;
    }
    values.append(valueStr);
    types.append(type.typeCode());
  }

  /**
   * Compares two docids. If the docids are unequal, then it parses the 
   * values out and compares them individually. Numeric values are compared 
   * numerically, all other values are compared lexigraphically.
   *
   * @param valueOrdering used to determine sort order of NULLs and text
   * @param docid1
   * @param docid2
   * @return a negative integer, zero, or a positive integer indicating
   *         whether docid1 is less than, equal to, or greater than docid2.
   */
  public static int compare(ValueOrdering valueOrdering, String docid1,
                            String docid2) {
    if (docid1.equals(docid2)) {
      return 0;
    }
    
    String[] tokens1 = docid1.split(PRIMARY_KEYS_SEPARATOR, -1);
    String[] tokens2 = docid2.split(PRIMARY_KEYS_SEPARATOR, -1);

    // Handle legacy format docids.
    if (tokens1.length == 1) {
      return (tokens2.length == 1) ? docid1.compareTo(docid2) : -1;
    } else if (tokens2.length == 1) {
      return 1;
    }

    for (int i = 1; i < tokens1.length && i < tokens2.length; i++) {
      Type type1 = Type.valueOf(tokens1[0].charAt(i - 1));
      Type type2 = Type.valueOf(tokens2[0].charAt(i - 1));
      int retval;

      // The most common case should be the types are the same.
      if (type1 == type2) {
        retval = compareLikeTypes(valueOrdering, type1, tokens1[i], tokens2[i]);
      } else {
        // The types are different?
        // Watch out for null values.
        if (type1 == Type.NULL) {
          retval = valueOrdering.nullsAreSortedLow() ? -1 : 1;
        } else if (type2 == Type.NULL) {
          retval = valueOrdering.nullsAreSortedLow() ? 1 : -1;
        } else if (type1.isNumeric() && type2.isNumeric()) {
          // If they are different types of numbers, compare them numerically.
          retval = new BigDecimal(tokens1[i])
                   .compareTo(new BigDecimal(tokens2[i]));
        } else {
          // Compare mis-matched types as Strings.
          String value1 =
              (type1 == Type.STRING) ? urlDecode(tokens1[i]) : tokens1[i];
          String value2 =
              (type2 == Type.STRING) ? urlDecode(tokens2[i]) : tokens2[i];
          retval = valueOrdering.getCollator().compare(value1, value2);
        }
      }
      if (retval != 0) {
        return retval;
      }
    }
    // If we got here, one docid matches the beginning of another docid.
    return tokens1.length - tokens2.length;
  }

  private static int compareLikeTypes(ValueOrdering valueOrdering, Type type,
                                      String value1, String value2) {
    switch (type) {
      case NULL:
        return 0;
      case LONG:
        return Long.valueOf(value1).compareTo(Long.valueOf(value2));
      case DOUBLE:
        return Double.valueOf(value1).compareTo(Double.valueOf(value2));
      case BIGDEC:
        return new BigDecimal(value1).compareTo(new BigDecimal(value2));
      case BIGINT:
        return new BigInteger(value1).compareTo(new BigInteger(value2));
      case STRING:
        return valueOrdering.getCollator()
               .compare(urlDecode(value1), urlDecode(value2));
      default:  // All the ISO 8601 dates/times sort lexigraphically.
        return value1.compareTo(value2);
    }
  }

  private static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // Should not happen with UTF-8.
      throw new AssertionError(e);
    }
  }

  private static String urlDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // Should not happen with UTF-8.
      throw new AssertionError(e);
    }
  }
}
