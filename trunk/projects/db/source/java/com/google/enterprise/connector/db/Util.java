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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.enterprise.connector.util.InputStreamFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.logging.Logger;

/** Byte array and string utilities. */
public class Util {
  private static final Logger LOG = Logger.getLogger(Util.class.getName());

  public static final String PRIMARY_KEY_SEPARATOR = ",";

  public static final Splitter PRIMARY_KEY_SPLITTER =
      Splitter.on(PRIMARY_KEY_SEPARATOR).trimResults();

  public static final Joiner PRIMARY_KEY_JOINER =
      Joiner.on(PRIMARY_KEY_SEPARATOR);

  public static final String CHECKSUM_ALGO = "SHA1";

  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  // This class should not be initialized.
  private Util() {
  }

  /** Gets whether a value is null, empty, or contains only whitespace. */
  public static boolean isNullOrWhitespace(String value) {
    return value == null || value.trim().isEmpty();
  }

  /**
   * Trims leading and trailing whitespace from the given string.
   *
   * @return null if the value is null, empty, or contains only
   *     whitespace, and the trimmed value otherwise.
   */
  public static String nullOrTrimmed(String value) {
    return (isNullOrWhitespace(value)) ? null : value.trim();
  }

  public static int indexOfIgnoreCase(List<String> values, String target) {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i).equalsIgnoreCase(target)) {
        return i;
      }
    }
    return -1;
  }

  public static String findIgnoreCase(Iterable<String> values, String target) {
    for (String value : values) {
      if (value.equalsIgnoreCase(target)) {
        return value;
      }
    }
    return null;
  }

  /**
   * Gets the case-preserving values of the primary key column names
   * from the actual column names.
   */
  public static ImmutableList<String> getCanonicalPrimaryKey(
      String configNames, Iterable<String> actualNames) {
    ImmutableList.Builder<String> canonicalNames = ImmutableList.builder();
    for (String configName : PRIMARY_KEY_SPLITTER.split(configNames)) {
      String result = findIgnoreCase(actualNames, configName.trim());
      if (result == null) {
        return null;
      } else {
        canonicalNames.add(result);
      }
    }
    return canonicalNames.build();
  }

  /**
   * Generates the SHA1 checksum.
   *
   * @param bufs arrays of bytes to checksum
   * @return checksum string.
   */
  public static String getChecksum(byte[]... bufs) {
    MessageDigest digest;
    try {
      digest = MessageDigest.getInstance(CHECKSUM_ALGO);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not get a message digest for "
          + CHECKSUM_ALGO + "\n" + e);
    }
    for (byte[] buf : bufs) {
      digest.update(buf);
    }
    return asHex(digest.digest());
  }

  /**
   * Utility method to convert a byte[] to hex string.
   *
   * @param buf
   * @return hex string.
   */
  public static String asHex(byte[] buf) {
    char[] chars = new char[2 * buf.length];
    for (int i = 0; i < buf.length; ++i) {
      chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
      chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
    }
    return new String(chars);
  }

  /**
   * Converts the InputStreamFactory into byte array.
   *
   * @param length
   * @param isFactory
   * @return byte array of Input Stream from factory, or null if there was an
   *         error
   */
  public static byte[] getBytes(int length, InputStreamFactory isFactory) {
    byte[] content = new byte[length];
    try {
      ByteStreams.read(isFactory.getInputStream(), content, 0, length);
    } catch (IOException e) {
      LOG.warning("Exception occurred while converting InputStreamFactory into "
                  + "byte array: " + e.toString());
      return null;
    }
    return content;
  }

  /**
   * Converts the InputStream into byte array.
   *
   * @param length
   * @param inStream
   * @return byte array of Input Stream, or null if there was an error
   */
  public static byte[] getBytes(int length, InputStream inStream) {
    byte[] content = new byte[length];
    try {
      ByteStreams.read(inStream, content, 0, length);
    } catch (IOException e) {
      LOG.warning("Exception occurred while converting InputStream into "
                  + "byte array: " + e.toString());
      return null;
    }
    return content;
  }

  /**
   * Converts the Character Reader into a UTF-8 encoded byte array.
   *
   * @param length the length (in characters) of the content to be read
   * @param reader a character Reader
   * @return byte array of read data, or null if there was an error
   */
  public static byte[] getBytes(int length, Reader reader) {
    int charsRead = 0;
    ByteArrayOutputStream content = new ByteArrayOutputStream(length *2);
    char[] chars = new char[Math.min(length, 32 * 1024)];
    while (charsRead < length) {
      try {
        int result = reader.read(chars);
        if (result == -1) {
          break;
        }
        charsRead += result;
        // Convert the chars to bytes in UTF-8 representation.
        byte[] bytes = (new String(chars, 0, result)).getBytes("UTF-8");
        content.write(bytes);
      } catch (IOException e) {
        LOG.warning("Exception occurred while converting character reader into"
            + " byte array: " + e.toString());
        return null;
      }
    }
    return content.toByteArray();
  }
}
