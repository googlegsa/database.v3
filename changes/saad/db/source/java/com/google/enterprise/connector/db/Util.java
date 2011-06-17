// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import com.google.enterprise.connector.db.diffing.JsonDocumentUtil;
import com.google.enterprise.connector.db.diffing.JsonObjectUtil;
import com.google.enterprise.connector.spi.SpiConstants;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Utility class for @ {@link JsonDocumentUtil}.
 */
public class Util {
  public static final String NO_TIMESTAMP = "NO_TIMESTAMP";
  public static final String NO_DOCID = "NO_DOCID";
  private static final Logger LOG = Logger.getLogger(Util.class.getName());
  public static final String PRIMARY_KEYS_SEPARATOR = ",";
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
  private static final String CHECKSUM_ALGO = "SHA1";
  private static final String DBCONNECTOR_PROTOCOL = "dbconnector://";
  private static final String DATABASE_TITLE_PREFIX = "Database Connector Result";
  public static final String ROW_CHECKSUM = "dbconnector:checksum";

  public static String WITH_BASE_URL = "withBaseURL";

  // This class should not be initialized.
  private Util() {
  }

  public static String getDisplayUrl(String hostname, String dbName,
      String docId) {
    // displayurl is of the form -
    // dbconnector://example.com/mysql/2a61639c96ed45ec8f6e3d4e1ab79944cd1d1923
    String displayUrl = String.format("%s%s/%s/%s", DBCONNECTOR_PROTOCOL, hostname, dbName, docId);
    return displayUrl;
  }

  /**
   * Generates the title of the DB document.
   *
   * @param primaryKeys primary keys of the database.
   * @param row row corresponding to the document.
   * @return title String.
   */
  public static String getTitle(String[] primaryKeys, Map<String, Object> row)
      throws DBException {
    StringBuilder title = new StringBuilder();
    title.append(DATABASE_TITLE_PREFIX).append(" ");

    if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
      Set<String> keySet = row.keySet();
      for (String primaryKey : primaryKeys) {
        /*
         * Primary key value is mapped to the value of key of map row before
         * getting record. We need to do this because GSA admin may entered
         * primary key value which differed in case from column name.
         */
        for (String key : keySet) {
          if (primaryKey.equalsIgnoreCase(key)) {
            primaryKey = key;
            break;
          }
        }
        if (!keySet.contains(primaryKey)) {
          String msg = "Primary Key does not match with any of the coulmn names";
          LOG.info(msg);
          throw new DBException(msg);
        }
        Object keyValue = row.get(primaryKey);
        String strKeyValue;
        if (keyValue == null || keyValue.toString().trim().length() == 0) {
          strKeyValue = "";
        } else {
          strKeyValue = keyValue.toString();
        }
        title.append(primaryKey).append("=");
        title.append(strKeyValue).append(" ");
      }
    } else {
      String msg = "";
      if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
        msg = "row is null and primary key array is empty";
      } else if (row != null) {
        msg = "hash map row is null";
      } else {
        msg = "primary key array is empty or null";
      }
      LOG.info(msg);
      throw new DBException(msg);
    }
    return title.toString();
  }

  /**
   * Generates the SHA1 checksum.
   *
   * @param buf
   * @return checksum string.
   */
  public static String getChecksum(byte[] buf) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(CHECKSUM_ALGO);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not get a message digest for "
          + CHECKSUM_ALGO + "\n" + e);
    }
    digest.update(buf);
    return asHex(digest.digest());
  }

  /**
   * Utility method to convert a byte[] to hex string.
   *
   * @param buf
   * @return hex string.
   */
  private static String asHex(byte[] buf) {
    char[] chars = new char[2 * buf.length];
    for (int i = 0; i < buf.length; ++i) {
      chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
      chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
    }
    return new String(chars);
  }

  /**
   * This method set the values for predefined Document properties For example
   * PROPNAME_DISPLAYURL , PROPNAME_TITLE , PROPNAME_LASTMODIFIED.
   *
   * @param row Map representing database row
   * @param hostname connector host name
   * @param dbName database name
   * @param docId document id of DB doc
   * @param isContentFeed true if Feed type is content feed
   */
  public static void setOptionalProperties(Map<String, Object> row,
      JsonObjectUtil jsonObjectUtil, DBContext dbContext) {
    if (dbContext == null) {
      return;
    }
    // set Document Title
    Object docTitle = row.get(dbContext.getDocumentTitle());
    if (docTitle != null) {
      jsonObjectUtil.setProperty(SpiConstants.PROPNAME_TITLE, docTitle.toString());

    }
    // set last modified date
    Object lastModified = row.get(dbContext.getLastModifiedDate());
    if (lastModified != null && (lastModified instanceof Timestamp)) {
      jsonObjectUtil.setLastModifiedDate(SpiConstants.PROPNAME_LASTMODIFIED, (Timestamp) lastModified);

    }
  }

  /**
   * This method will add value of each column as metadata to Database document
   * except the values of columns in skipColumns list.
   *
   * @param doc
   * @param row
   * @param skipColumns list of columns needs to ignore while indexing
   */
  public static void setMetaInfo(JsonObjectUtil jsonObjectUtil,
      Map<String, Object> row, List<String> skipColumns) {
    // get all column names as key set
    Set<String> keySet = row.keySet();
    for (String key : keySet) {
      // set column value as metadata and column name as meta-name.
      if (!skipColumns.contains(key)) {
        Object value = row.get(key);
        if (value != null)
          jsonObjectUtil.setProperty(key, value.toString());

      } else {
        LOG.info("skipping metadata indexing of column " + key);
      }
    }
  }

  /**
   * this method copies all elements from map representing a row except BLOB
   * column and return the resultant map.
   *
   * @param row
   * @return map representing a database table row.
   */
  public static Map<String, Object> getRowForXmlDoc(Map<String, Object> row,
      DBContext dbContext) {
    Set<String> keySet = row.keySet();
    Map<String, Object> map = new HashMap<String, Object>();
    for (String key : keySet) {
      if (!dbContext.getLobField().equals(key)) {
        map.put(key, row.get(key));
      }
    }
    return map;
  }

  /**
   * This method extract the columns for Last Modified date and Document Title
   * and add in list of skip columns.
   *
   * @param skipColumns list of columns to be skipped as metadata
   * @param dbContext
   */

  public static void skipOtherProperties(List<String> skipColumns,
      DBContext dbContext) {
    String lastModColumn = dbContext.getLastModifiedDate();
    String docTitle = dbContext.getDocumentTitle();
    if (lastModColumn != null && lastModColumn.trim().length() > 0) {
      skipColumns.add(lastModColumn);
    }
    if (docTitle != null && docTitle.trim().length() > 0) {
      skipColumns.add(docTitle);
    }
  }

  /**
   * This method converts the Input AStream into byte array.
   *
   * @param length
   * @param inStream
   * @return byte array of Input Stream
   */
  public static byte[] getBytes(int length, InputStream inStream) {

    int bytesRead = 0;
    byte[] content = new byte[length];
    while (bytesRead < length) {
      int result;
      try {
        result = inStream.read(content, bytesRead, length - bytesRead);
        if (result == -1)
          break;
        bytesRead += result;
      } catch (IOException e) {
        LOG.warning("Exception occurred while converting InputStream into byte array"
            + e.toString());
        return null;
      }
    }
    return content;
  }

  /**
   * This method sets the content of blob data in JsonDocument.
   *
   * @param blobContent BLOB content to be set
   * @param dbName name of the database
   * @param row Map representing row in the database table
   * @param dbContext object of DBContext
   * @param primaryKeys primary key columns
   * @return JsonDocument
   * @throws DBException
   */
  public static JsonObjectUtil setBlobContent(byte[] blobContent,
      JsonObjectUtil jsonObjectUtil, String dbName, Map<String, Object> row,
      DBContext dbContext, String[] primaryKeys, String docId)
      throws DBException {

    jsonObjectUtil.setBinaryContent(SpiConstants.PROPNAME_CONTENT, blobContent);
    // get xml representation of document(exclude the BLOB column).
    Map<String, Object> rowForXmlDoc = getRowForXmlDoc(row, dbContext);
    String xmlRow = XmlUtils.getXMLRow(dbName, rowForXmlDoc, primaryKeys, "", dbContext, true);
    // get checksum of blob
    String blobCheckSum = Util.getChecksum((blobContent));
    // get checksum of other column
    String otherColumnCheckSum = Util.getChecksum(xmlRow.getBytes());
    // get checksum of blob object and other column
    String docCheckSum = Util.getChecksum((otherColumnCheckSum + blobCheckSum).getBytes());
    // set checksum of this document
    jsonObjectUtil.setProperty(ROW_CHECKSUM, docCheckSum);
    LOG.info("BLOB Data found");

    return jsonObjectUtil;
  }
}
