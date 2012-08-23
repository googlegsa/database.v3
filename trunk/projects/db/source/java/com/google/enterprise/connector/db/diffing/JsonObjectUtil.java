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

import com.google.common.io.FileBackedOutputStream;
import com.google.common.io.InputSupplier;
import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.InputStreamFactory;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A utility class for creating JSONObject object and setting its property for
 * creating JsonDocument objects.
 */
public class JsonObjectUtil {

  private static final Logger LOG =
      Logger.getLogger(JsonObjectUtil.class.getName());
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
      try {
        properties.put(propertyName, Collections.singletonList(
            Value.getStringValue(propertyValue)));
        jsonObject.put(propertyName, new SimpleProperty(
            Collections.singletonList(Value.getStringValue(propertyValue)))
            .nextValue().toString());
      } catch (JSONException e) {
        LOG.warning("Exception for " + propertyName + " with value "
            + propertyValue + "\n" + e.toString());
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
    Timestamp time = propertyValue;
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time.getTime());

    if (propertyValue == null) {
      return;
    }
    try {
      properties.put(propertyName,
          Collections.singletonList(Value.getDateValue(cal)));
      jsonObject.put(propertyName, new SimpleProperty(
          Collections.singletonList(Value.getDateValue(cal))).nextValue()
          .toString());
    } catch (JSONException e) {
      LOG.warning("Exception for " + propertyName + " with value "
          + propertyValue + "\n" + e.toString());
    }
  }

  /**
   * In case of BLOB data iBATIS returns binary array for BLOB data-type. This
   * method sets the "binary array" as a content of JSON Object.
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
      properties.put(propertyName, Collections.singletonList(
          Value.getBinaryValue(isf)));
    } catch (IOException e) {
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.WARNING, "Failed to cache document content for "
                + propertyName, e);
      } else {
        LOG.warning("Failed to cache document content for " + propertyName 
                    + ":\n" + e.toString());
      }
      // Resort to holding the binary data in memory, rather than on disk.
      properties.put(propertyName, Collections.singletonList(
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
