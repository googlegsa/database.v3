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

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.util.InputStreamFactory;
import com.google.enterprise.connector.util.MimeTypeDetector;

import java.io.InputStream;
import java.io.IOException;

/**
 * Detector for mime type based on file name and content.
 */
public class MimeTypeFinder {

  private static final MimeTypeFinder mimeTypeFinder = new MimeTypeFinder();
  private final MimeTypeDetector mimeTypeDetector;

  public static MimeTypeFinder getInstance() {
    return mimeTypeFinder;
  }

  private MimeTypeFinder() {
    mimeTypeDetector = new MimeTypeDetector();
  }

  /**
   * Used by the tests to inject a test traversalContext.
   * In production, the traversal context is injected by Spring during
   * Connector Manager startup.
   */
  @VisibleForTesting
  void setTraversalContext(TraversalContext traversalContext) {
    MimeTypeDetector.setTraversalContext(traversalContext);
  }

  /**
   * Returns the mime type for the file with the provided name and content.
   *
   * @throws IOException
   */
  public String getMimeType(final InputStream file) throws IOException {
    return mimeTypeDetector.getMimeType(null,
        new InputStreamFactory() {
          public InputStream getInputStream() {
            return file;
          }
        });
  }
}
