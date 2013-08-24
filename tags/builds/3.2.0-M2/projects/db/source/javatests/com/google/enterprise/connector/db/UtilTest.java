// Copyright 2012 Google Inc.
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

import java.io.StringReader;

import junit.framework.TestCase;

public class UtilTest extends TestCase {

  // Looks like "Chinese glyphs -> tiger eye"
  private static final String UNICODE_STRING = "\u0187\u0127\u012F\u0148\u0207"
     + "\u0219\u0229 \u0261\u026D\u01B4\u01A5\u0195\u0282 \u2192 \u2EC1 \u2EAC";
  
  /** Test getBytes(int, Reader) */
  public void testGetBytesReader() throws Exception {
    testGetBytesReader(UNICODE_STRING.length(), UNICODE_STRING);
  }

  /** Test getBytes(int, Reader) with short read (EOF). */
  public void testGetBytesReaderEof() throws Exception {
    testGetBytesReader(UNICODE_STRING.length() * 2, UNICODE_STRING);
  }

  /** Test getBytes(int, Reader) with short read (EOF). */
  public void testGetBytesReaderEmptyContent() throws Exception {
    testGetBytesReader(0, "");
  }

  /** Test getBytes(int, Reader) with input larger than internal buffer. */
  public void testGetBytesReaderLargeInput() throws Exception {
    StringBuilder builder = new StringBuilder();
    // Generate a 100,000 character String, bigger than the 32K buffer.
    for (int i = 0; i < 5000; i++) {
      builder.append(UNICODE_STRING);
    }
    testGetBytesReader(builder.length(), builder.toString());    
  }
  
  /**
   * Tests that getBytes(int, Reader) returns a UTF-8 encoded byte array
   * representation of the test String.
   */
  private void testGetBytesReader(int length, String expected) throws Exception {
    byte[] bytes = Util.getBytes(length, new StringReader(expected));
    assertNotNull(bytes);
    String actual = new String(bytes, "UTF-8");
    assertEquals(actual, expected, actual);
  }
}

