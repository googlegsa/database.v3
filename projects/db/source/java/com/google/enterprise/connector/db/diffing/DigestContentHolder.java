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

import com.google.enterprise.connector.db.Util;
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
  private final MessageDigest digest;
  private String checksum;

  public DigestContentHolder(Object content, String mimeType) {
    super(content, null, mimeType);
    this.checksum = null;
    try {
      this.digest = MessageDigest.getInstance(Util.CHECKSUM_ALGO);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not get a message digest for "
                                 + Util.CHECKSUM_ALGO, e);
    }
  }

  @Override
  public synchronized String getChecksum() {
    if (checksum == null) {
      checksum = Util.asHex(digest.digest());
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
