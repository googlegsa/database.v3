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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.spi.Value;

import org.json.JSONException;
import org.json.JSONObject;

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
  private static TraversalContext traversalContext;

  public static void setTraversalContext(TraversalContext traversalContext) {
    JsonDocument.traversalContext = traversalContext;
  }

  /**
   * Constructor used by {@link DBHandle} when deserializing a
   * {@code DocumentHandle} from the recovery file.
   */
  public JsonDocument(JSONObject jsonObject) {
    this(buildJsonProperties(jsonObject), jsonObject);
  }

  /**
   * Constructor used by the {@link DocumentBuilder} for creating a
   * {@link JsonDocument} object used by {@link RepositoryHandler}
   * for building a collection over JsonDocument.
   */
  public JsonDocument(Map<String, List<Value>> properties,
                      JSONObject jsonObject) {
    this.properties = properties;
    jsonString = jsonObject.toString();
    objectId = getSingleValueString(SpiConstants.PROPNAME_DOCID);
    if (Strings.isNullOrEmpty(objectId)) {
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
        ImmutableMap.builder();
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
      ImmutableMap.Builder<String, List<Value>> mapBuilder, String key) {
    try {
      if (!jo.isNull(key)) {
        mapBuilder.put(key,
            ImmutableList.of(Value.getStringValue(jo.getString(key))));
      }
    } catch (JSONException e) {
      LOG.warning("Exception thrown while extracting key: " + key + "\n" + e);
    }
  }

  private static void extractDocid(JSONObject jo,
      ImmutableMap.Builder<String, List<Value>> mapBuilder) {
    String docid;
    try {
      docid = jo.getString(SpiConstants.PROPNAME_DOCID);
      Preconditions.checkState(!Strings.isNullOrEmpty(docid));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Internal consistency error: missing docid", e);
    }
    mapBuilder.put(SpiConstants.PROPNAME_DOCID,
                   ImmutableList.of(Value.getStringValue(docid)));
  }

  @Override
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  @Override
  public Property findProperty(String name) throws RepositoryException {
    List<Value> property = properties.get(name);
    if (name.equals(SpiConstants.PROPNAME_CONTENT) && filterMimeType()) {
        property = null;
    }
    return (property == null) ? null : new SimpleProperty(property);
  }

  /**
   * Filter the Document or just its Content based upon its MIME type.
   *
   * @return true if content should be skipped based upon its MIME type,
   *         false otherwise.
   * @throws SkippedDocumentException if this document is to be ignored
   *         based upon its MIME type.
   */
  private boolean filterMimeType() throws RepositoryException {
    String mimeType = getSingleValueString(SpiConstants.PROPNAME_MIMETYPE);
    if (mimeType != null && traversalContext != null) {
      int mimeTypeSupportLevel =
          traversalContext.mimeTypeSupportLevel(mimeType);
      if (mimeTypeSupportLevel == 0) {
        LOG.warning("Skipping the contents with docId: " + objectId
            + " as content MIME type " + mimeType + " is not supported.");
        return true;
      } else if (mimeTypeSupportLevel < 0) {
        String msg = new StringBuilder("Skipping the document with docId: ")
            .append(objectId).append(" as the MIME type ").append(mimeType)
            .append(" is in the 'ignored' MIME types list.").toString();
        LOG.warning(msg);
        throw new SkippedDocumentException(msg);
      }
    }
    return false;
  }

  /** Returns the first value of the named property as a String. */
  private String getSingleValueString(String name) {
    List<Value> values = properties.get(name);
    if (values != null) {
      Value value = values.iterator().next();
      if (value != null) {
        return value.toString();
      }
    }
    return null;
  }
}
