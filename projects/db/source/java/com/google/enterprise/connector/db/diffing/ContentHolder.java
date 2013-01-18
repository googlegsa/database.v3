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

/**
 * Holds the content in some form, along with the derived content
 * metadata. This is used to delegate to the subclasses how and when
 * they materialize the content, and allowing them to produce the
 * metadata whenever it is most expedient.
 */
/*
 * TODO(jlacey): Should the content type be genericized (Object -> T) or
 * refined to a particular concrete type?
 */
public class ContentHolder {
  private final Object content;
  private final String checksum;
  private final String mimeType;

  public ContentHolder(Object content, String checksum, String mimeType) {
    this.content = content;
    this.checksum = checksum;
    this.mimeType = mimeType;
  }

  public Object getContent() {
    return content;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getMimeType() {
    return mimeType;
  }
}
