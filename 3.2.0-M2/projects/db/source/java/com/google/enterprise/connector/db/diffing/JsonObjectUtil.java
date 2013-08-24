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
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.InputStreamFactory;

import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private final JSONObject jsonObject;

  public JsonObjectUtil() {
    jsonObject = new JSONObject();
  }

  public Map<String, List<Value>> getProperties() {
    return properties;
  }

  public JSONObject getJsonObject() {
    return jsonObject;
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
  public void setBinaryContent(String propertyName,
      InputStreamFactory propertyValue) {
    if (propertyValue == null) {
      return;
    }
    properties.put(propertyName,
        ImmutableList.of(Value.getBinaryValue(propertyValue)));
    try {
      jsonObject.put(propertyName, propertyValue);
    } catch (JSONException e) {
      LOG.warning("Exception for " + propertyName + " with value "
          + propertyValue + "\n" + e.toString());
    }
  }
}
