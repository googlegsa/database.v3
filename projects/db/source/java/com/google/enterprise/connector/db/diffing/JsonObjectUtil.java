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
import com.google.common.io.FileBackedOutputStream;
import com.google.common.io.InputSupplier;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A utility class for creating JSONObject object and setting its properties for
 * creating JsonDocument objects.
 */
// TODO(bmj): The current distinction between JsonDocument and JsonObjectUtil
// seems twisted. These should be refactored, separating the SPI
// Document/Property/Value functionality into DBDocument/DBDocumentUtil, and
// the Diffing snapshot functionality into the JsonObject/JsonObjectUtil.
// TODO(jlacey): I think a straightforward approach is to simply implement
// JsonDocument over a JSONObject, rather than build parallel property sets
// here. There are issues with the last modified date, which currently gets
// serialized as an ISO 8601 date and no longer works as an RFC 822 date for
// the feed lastModified attribute. There may be issues with LOB support as
// well.
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
      properties.put(propertyName,
          ImmutableList.of(Value.getStringValue(propertyValue)));
      try {
        jsonObject.put(propertyName, propertyValue);
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
    if (propertyValue != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(propertyValue.getTime());
      Value value = Value.getDateValue(cal);
      properties.put(propertyName, ImmutableList.of(value));
      try {
        jsonObject.put(propertyName, value.toString());
      } catch (JSONException e) {
        LOG.warning("Exception for " + propertyName + " with value "
            + propertyValue + "\n" + e.toString());
      }
    }
  }

  /**
   * This method sets the "binary array" property value as the content of
   * JSON Object.
   *
   * @param propertyName
   * @param propertyValue
   */
  public void setBinaryContent(String propertyName, Value propertyValue) {
    if (propertyValue == null) {
      return;
    }
    properties.put(propertyName, ImmutableList.of(propertyValue));
  }

  /**
   * This method gets the "binary array" property value.
   *
   * @param propertyValue
   */
  public static Value getBinaryValue(byte[] propertyValue) {
    if (propertyValue == null) {
      return null;
    }
    try {
      InputStreamFactory isf = new FileBackedInputStreamFactory(propertyValue);
      return Value.getBinaryValue(isf);
    } catch (IOException e) {
      if (LOG.isLoggable(Level.FINEST)) {
        LOG.log(Level.WARNING, "Failed to cache document content.", e);
      } else {
        LOG.warning("Failed to cache document content:\n" + e.toString());
      }
      // Resort to holding the binary data in memory, rather than on disk.
      return Value.getBinaryValue(propertyValue);
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
