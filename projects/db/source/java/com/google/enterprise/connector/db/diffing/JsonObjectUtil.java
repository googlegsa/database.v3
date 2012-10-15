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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.FileBackedOutputStream;
import com.google.common.io.InputSupplier;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.InputStreamFactory;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A utility class for creating JSONObject object and setting its properties for
 * creating JsonDocument objects.
 * Note that the JSON object is used for the Diffing connector snapshot and 
 * contains only a small subset of the JsonDocument's properties, specifically
 * just the DocId and the Checksum used to detect changes.
 */
// TODO (bmj): The current distinction between JsonDocument, DocumentBuilder,
// and JsonObjectUtil seems twisted. These should be refactored, separating the
// SPI Document/Property/Value functionality into DBDocument/DBDocumentUtil,
// and the Diffing snapshot functionality into the JsonObject/JsonObjectUtil.
public class JsonObjectUtil {

  private static final Logger LOG =
      Logger.getLogger(JsonObjectUtil.class.getName());

  /** The fields included only in the JSON snapshot object. */
  private static final Set<String> SNAPSHOT_ONLY_FIELDS = 
      ImmutableSet.of(DocumentBuilder.ROW_CHECKSUM);

  /** The fields included in the JSON snapshot object. */
  private static final Set<String> SNAPSHOT_FIELDS = 
      ImmutableSet.<String>builder().add(SpiConstants.PROPNAME_DOCID)
          .addAll(SNAPSHOT_ONLY_FIELDS).build();

  private final Map<String, List<Value>> properties =
      new HashMap<String, List<Value>>();

  public Map<String, List<Value>> getProperties() {
    return properties;
  }

  private JSONObject jsonObject;

  public JsonObjectUtil() {
    jsonObject = new JSONObject();
  }

  /**
   * Set a property for JSONObject. If propertyValue is null this does nothing.
   *
   * @param propertyName
   * @param propertyValue
   */
  public void setProperty(String propertyName, String propertyValue) {
    if (propertyValue != null) {
      if (!SNAPSHOT_ONLY_FIELDS.contains(propertyName)) {
        properties.put(propertyName,
            ImmutableList.of(Value.getStringValue(propertyValue)));
      }
      if (SNAPSHOT_FIELDS.contains(propertyName)) {
        try {
          jsonObject.put(propertyName, propertyValue);
        } catch (JSONException e) {
          LOG.warning("Exception for " + propertyName + " with value "
                      + propertyValue + "\n" + e.toString());
        }
      }
    }
  }

  /**
   * Adds the last modified date property to the JSON Object.
   *
   * @param propertyName
   * @param propertyValue
   */
  public void setLastModifiedDate(String propertyName, Timestamp propertyValue) {
    if (propertyValue != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(propertyValue.getTime());
      properties.put(propertyName, ImmutableList.of(Value.getDateValue(cal)));
    }
  }

  /**
   * This method sets the "binary array" property value as the content of
   * JSON Object.
   *
   * @param propertyName
   * @param propertyValue
   */
  public void setBinaryContent(String propertyName, byte[] propertyValue) {
    if (propertyValue == null) {
      return;
    }
    try {
      InputStreamFactory isf = new FileBackedInputStreamFactory(propertyValue);
      properties.put(propertyName, ImmutableList.of(Value.getBinaryValue(isf)));
    } catch (IOException e) {
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.WARNING, "Failed to cache document content for "
                + propertyName, e);
      } else {
        LOG.warning("Failed to cache document content for " + propertyName 
                    + ":\n" + e.toString());
      }
      // Resort to holding the binary data in memory, rather than on disk.
      properties.put(propertyName, ImmutableList.of(
          Value.getBinaryValue(propertyValue)));
    }
  }

  public JSONObject getJsonObject() {
    return jsonObject;
  }

  /** An InputStreamFactory backed by a FileBackedOutputStream. */
  private static class FileBackedInputStreamFactory
      implements InputStreamFactory {
    private static int IN_MEMORY_THRESHOLD = 32 * 1024;

    /**
     * We hold onto a single supplier, because when that gets finalized,
     * the backing file will get deleted.
     */
    private InputSupplier<InputStream> supplier;

    FileBackedInputStreamFactory(byte[] data) throws IOException {
      FileBackedOutputStream out =
          new FileBackedOutputStream(IN_MEMORY_THRESHOLD, true);
      out.write(data);
      out.close();
      supplier = out.getSupplier();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return supplier.getInput();
    }
  }
}
