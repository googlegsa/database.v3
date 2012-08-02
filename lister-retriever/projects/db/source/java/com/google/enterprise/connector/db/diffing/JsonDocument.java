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

package com.google.enterprise.connector.db.diffing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.db.MimeTypeFinder;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryDocumentException;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.spiimpl.BinaryValue;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A simple {@link Document} implementation created from a {@link JSONObject}.
 */
public class JsonDocument implements Document {
  private static final Logger LOG =
      Logger.getLogger(JsonDocument.class.getName());

  private final String jsonString;
  private final String objectId;
  private final Map<String, List<Value>> properties;
  private boolean changed = false;
  private static TraversalContext traversalContext;

  public static void setTraversalContext(TraversalContext traversalContext) {
    JsonDocument.traversalContext = traversalContext;
  }

  /**
   * Constructor used by {@link JsonDocumentUtil} for creating a
   * {@link JsonDocument} object used by {@link RepositoryHandler}
   * for building a collection over JsonDocument.
   */
  public JsonDocument(Map<String, List<Value>> properties,
                      JSONObject jsonObject) {
    this.properties = properties;
    jsonString = jsonObject.toString();
    try {
      objectId = Value.getSingleValueString(this, SpiConstants.PROPNAME_DOCID);
    } catch (RepositoryException e) {
      throw new IllegalStateException("Internal consistency error", e);
    }
    if (objectId == null) {
      throw new IllegalArgumentException(
          "Unable to parse for docID from the JSON string:" + jsonString);
    }

  }

  public void setChanged() {
    this.changed = true;
  }

  public boolean getChanged() {
    return this.changed;
  }

  /**
   * Constructor used by {@link DBClass} for creating {@link JsonDocument}
   * for change detection purposes.
   */
  public JsonDocument(JSONObject jsonObject) {
    this.properties = buildJsonProperties(jsonObject);
    jsonString = jsonObject.toString();
    try {
      objectId = Value.getSingleValueString(this, SpiConstants.PROPNAME_DOCID);
    } catch (RepositoryException e) {
      throw new IllegalStateException("Internal consistency error", e);
    }
    if (objectId == null) {
      throw new IllegalArgumentException(
          "Unable to parse for docID from the JSON string:" + jsonString);
    }
  }

  public String getDocumentId() {
    return objectId;
  }

  public String toJson() {
    return jsonString;
  }

  /**
   * A class level method for extracting attributes from JSONObject object and
   * creating a {@code Map<String,List<Value>>} used by the superclass({@link
   * SimpleDocument}) constructor and hence creating a JsonDocument Object.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, List<Value>> buildJsonProperties(JSONObject jo) {
    ImmutableMap.Builder<String, List<Value>> mapBuilder =
        new ImmutableMap.Builder<String, List<Value>>();
    Iterator<String> jsonKeys = jo.keys();
    while (jsonKeys.hasNext()) {

      String key = jsonKeys.next();
      if (key.equals(SpiConstants.PROPNAME_DOCID)) {
        extractDocid(jo, mapBuilder);
      } else {
        extractAttributes(jo, mapBuilder, key);
      }
    }

    return mapBuilder.build();
  }

  /**
   * A class level method for extracting attributes from JSONObject object used
   * by {@link #buildJsonProperties}.
   */
  private static void extractAttributes(JSONObject jo,
      ImmutableMap.Builder<String, List<Value>> mapBuilder, String key)
      throws IllegalAccessError {
    String ja;
    try {
      ja = (String) jo.get(key);
    } catch (JSONException e) {
      LOG.warning("Exception thrown while extracting key: " + key + "\n" + e);
      return;
    }
    ImmutableList.Builder<Value> builder = new ImmutableList.Builder<Value>();
    builder.add(Value.getStringValue(ja));
    ImmutableList<Value> l = builder.build();
    if (l.size() > 0) {
      mapBuilder.put(key, l);
    }
    return;
  }

  private static void extractDocid(JSONObject jo,
      ImmutableMap.Builder<String, List<Value>> mapBuilder) {
    String docid;
    try {
      docid = jo.getString(SpiConstants.PROPNAME_DOCID);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Internal consistency error: missing docid", e);
    }
    mapBuilder.put(SpiConstants.PROPNAME_DOCID,
                   ImmutableList.of(Value.getStringValue(docid)));
  }

  public Property findProperty(String name) throws RepositoryDocumentException {
    List<Value> property = properties.get(name);
    if (this.changed) {
      if (name.equals(SpiConstants.PROPNAME_CONTENT)) {
        int val = filterMimeType();
        if (val == 0)
          property = null;
      }
    }
    return (property == null) ? null : new SimpleProperty(property);
  }

  private int filterMimeType() throws RepositoryDocumentException {
    List<Value> mimeTypeProperty = properties.get(SpiConstants.PROPNAME_MIMETYPE);

    // Null mimeTypeProperty indicates MimeType detection needs to be done
    // for Blob content.
    if (mimeTypeProperty == null) {
      String docId = this.properties.get(SpiConstants.PROPNAME_DOCID)
          .iterator().next().toString();
      String mimeType = null;
      List<Value> content = this.properties.get(SpiConstants.PROPNAME_CONTENT);
      InputStream is =
          ((BinaryValue) content.iterator().next()).getInputStream();
      int mimeTypeSupportLevel = 1;

      try {
        mimeType = MimeTypeFinder.getInstance().getMimeType(is);
        is.close();
        this.properties.put(SpiConstants.PROPNAME_MIMETYPE,
            Collections.singletonList(Value.getStringValue(mimeType)));
        if (traversalContext != null) {
          mimeTypeSupportLevel =
              traversalContext.mimeTypeSupportLevel(mimeType);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      if (mimeTypeSupportLevel == 0) {
        LOG.warning("Skipping the contents with docId: " + docId
            + " as content mime type " + mimeType + " is not supported.");
        return 0;
      } else if (mimeTypeSupportLevel < 0) {
        String msg = new StringBuilder("Skipping the document with docId: ")
            .append(docId).append(" as the mime type ").append(mimeType)
            .append(" is in the 'ignored' mimetypes list.").toString();
        LOG.warning(msg);
        throw new SkippedDocumentException(msg);
      }
    }
    return -1;
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }
}
