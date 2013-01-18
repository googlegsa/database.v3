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

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.FileBackedOutputStream;
import com.google.common.io.InputSupplier;
import com.google.enterprise.connector.spi.RepositoryDocumentException;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.spiimpl.BinaryValue;
import com.google.enterprise.connector.util.Base64;
import com.google.enterprise.connector.util.Base64DecoderException;
import com.google.enterprise.connector.util.Base64FilterInputStream;
import com.google.enterprise.connector.util.InputStreamFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** A factory and utility class for {@code InputStreamFactory}. */
/* TODO(jlacey): Move this to CM util package if another connector wants it. */
public class InputStreamFactories {
  private static final Logger LOG =
      Logger.getLogger(InputStreamFactories.class.getName());

  /**
   * Gets an {@code InputStreamFactory} that tries not to consume large
   * amounts of memory, no matter how large the given byte array is.
   *
   * @param data a byte array
   */
  public static final InputStreamFactory newInstance(byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return new FileBackedInputStreamFactory(data);
    } catch (IOException e) {
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.WARNING, "Failed to cache document content.", e);
      } else {
        LOG.warning("Failed to cache document content:\n" + e.toString());
      }
      // Resort to holding the binary data in memory, rather than on disk.
      return new ByteArrayInputStreamFactory(data);
    }
  }

  /**
   * Gets an {@code InputStreamFactory} for a Base64-encoded string.
   * If the input is not Base64-encoded, it is converted to bytes
   * using UTF-8.
   */
  public static final InputStreamFactory fromBase64String(String content) {
    byte[] bytes;
    try {
      bytes = content.getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
    byte[] decodedBytes;
    try {
      decodedBytes = Base64.decode(bytes);
    } catch (Base64DecoderException e) {
      // Just leave the data as-is.
      decodedBytes = bytes;
    }
    return newInstance(decodedBytes);
  }

  /** Fully reads an input stream from the factory and Base64 encodes it. */
  public static final String toBase64String(InputStreamFactory factory)
      throws IOException {
    return CharStreams.toString(new InputStreamReader(
            new Base64FilterInputStream(factory.getInputStream()),
            Charsets.UTF_8));
  }

  /** Fully reads an input stream from the value and Base64 encodes it. */
  public static final String toString(Value binaryValue)
      throws IOException, RepositoryDocumentException {
    return CharStreams.toString(new InputStreamReader(
            ((BinaryValue) binaryValue).getInputStream(),
            Charsets.UTF_8));
  }

  public static interface ContentLengthInputStreamFactory
      extends InputStreamFactory {
    /**
     * Returns the number of bytes of content that will be returned by the
     * InputStream, or -1 if the length is not known.
     */
    public long length();
  }

  /** An InputStreamFactory backed by a FileBackedOutputStream. */
  private static class FileBackedInputStreamFactory
      implements ContentLengthInputStreamFactory {
    private static final int IN_MEMORY_THRESHOLD = 32 * 1024;

    /**
     * We hold onto a single supplier, because when that gets finalized,
     * the backing file will get deleted.
     */
    private final InputSupplier<InputStream> supplier;
    private final long length;

    FileBackedInputStreamFactory(byte[] data) throws IOException {
      FileBackedOutputStream out =
          new FileBackedOutputStream(IN_MEMORY_THRESHOLD, true);
      length = data.length;
      out.write(data);
      out.close();
      supplier = out.getSupplier();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return supplier.getInput();
    }

    @Override
    public long length() {
      return length;
    }
  }

  /** An InputStreamFactory backed by a byte array. */
  private static class ByteArrayInputStreamFactory
      implements ContentLengthInputStreamFactory {
    private final byte[] data;

    ByteArrayInputStreamFactory(byte[] data) {
      // TODO(jlacey): Make a defensive copy of the array?
      this.data = data;
    }

    @Override
    public InputStream getInputStream() {
      return new ByteArrayInputStream(data);
    }

    @Override
    public long length() {
      return data.length;
    }
  }

  /** This class should not be instantiated. */
  private InputStreamFactories() {
    throw new AssertionError();
  }
}
