// Copyright 2013 Google Inc.
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

import com.google.common.base.Preconditions;
import com.google.enterprise.connector.db.InputStreamFactories;
import com.google.enterprise.connector.db.Util;
import com.google.enterprise.connector.util.Base16;
import com.google.enterprise.connector.util.InputStreamFactory;
import com.google.enterprise.connector.util.MimeTypeDetector;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A {@link ContentHolder} that maintains an active MessageDigest to lazily
 * calculate the checksum. For some DocumentBuilder implementations, all
 * the components to calculating the checksum may not be readily available.
 * This keeps the MessageDigest active for updating until {@link #getChecksum()}
 * is called.
 */
public class DigestContentHolder extends ContentHolder {
  public static DigestContentHolder getInstance(byte[] contentBytes,
      MimeTypeDetector mimeTypeDetector) {
    Preconditions.checkNotNull(contentBytes);
    DigestContentHolder contentHolder = new DigestContentHolder(
        InputStreamFactories.newInstance(contentBytes),
        mimeTypeDetector.getMimeType(null, contentBytes),
        contentBytes.length);
    contentHolder.updateDigest(contentBytes);
    return contentHolder;
  }

  public static DigestContentHolder getEmptyInstance(String mimeType) {
    return new DigestContentHolder(
        InputStreamFactories.newInstance(new byte[0]), mimeType, 0);
  }

  private final int length;
  private final MessageDigest digest;
  private String checksum;

  private DigestContentHolder(InputStreamFactory content, String mimeType,
      int length) {
    super(content, null, mimeType);
    this.length = length;
    this.checksum = null;
    try {
      this.digest = MessageDigest.getInstance(Util.CHECKSUM_ALGO);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not get a message digest for "
                                 + Util.CHECKSUM_ALGO, e);
    }
  }

  public long getLength() {
    return length;
  }

  @Override
  public synchronized String getChecksum() {
    if (checksum == null) {
      checksum = Base16.lowerCase().encode(digest.digest());
    }
    return checksum;
  }

  /**
   * Updates the digest using the specified array of bytes.
   *
   * @param buf the array of bytes.
   */
  public synchronized void updateDigest(byte[] buf) {
    digest.update(buf);
  }
}
