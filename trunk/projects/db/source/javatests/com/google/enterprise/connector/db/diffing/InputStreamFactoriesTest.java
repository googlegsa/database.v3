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

package com.google.enterprise.connector.db.diffing;

import com.google.common.io.ByteStreams;
import com.google.enterprise.connector.util.InputStreamFactory;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class InputStreamFactoriesTest extends TestCase {
  /** Reads all the bytes from a {@code InputStreamFactory}. */
  private byte[] toByteArray(InputStreamFactory factory) throws IOException {
    return ByteStreams.toByteArray(factory.getInputStream());
  }

  /**
   * Tests running a byte array of the given size through the methods
   * of {@code InputStreamFactories}, which not coincidentally are
   * chainable. The use of Random is just for fun.
   */
 private void testRoundTrip(int size) throws IOException {
    byte[] data = new byte[size];
    new Random().nextBytes(data);

    assertTrue(Arrays.equals(data, toByteArray(
        InputStreamFactories.fromBase64String(
            InputStreamFactories.toBase64String(
                InputStreamFactories.newInstance(data))))));
  }

  /**
   * Test a byte array smaller than the internal cutoff in
   * FileBackedOutputStream.
   */
  public void testLittle() throws IOException {
    testRoundTrip(256);
  }

  /**
   * Test a byte array larger than the internal cutoff in
   * FileBackedOutputStream.
   */
  public void testBig() throws IOException {
    testRoundTrip(100000);
  }
}
