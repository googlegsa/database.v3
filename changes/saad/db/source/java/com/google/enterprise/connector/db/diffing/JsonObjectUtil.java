// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db.diffing;

import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.Value;

import org.json.JSONException;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.logging.Logger;

/**
 * A utility class for creating JSONObject object and setting its property for
 * creating JsonDocument objects.
 */
public class JsonObjectUtil {

	private static final Logger LOG = Logger.getLogger(JsonObjectUtil.class.getName());


    private JSONObject jsonObject;

    public JsonObjectUtil() {
		jsonObject = new JSONObject();
	}

    /**
	 * Set a property for JSONObject. If propertyValue is null this does
	 * nothing.
	 * 
	 * @param propertyName
	 * @param propertyValue
	 */
    public void setProperty(String propertyName, String propertyValue) {
		if (propertyValue != null) {
			try {
				jsonObject.put(propertyName, new SimpleProperty(
						Collections.singletonList(Value.getStringValue(propertyValue))).nextValue().toString());
			} catch (JSONException e) {
				LOG.warning("Exception for " + propertyName + " with value "
						+ propertyValue + "\n" + e);
			}
		}
	}

    /**
	 * This method adds the last modified date property to the JSON Object
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
			jsonObject.put(propertyName, new SimpleProperty(
					Collections.singletonList(Value.getDateValue(cal))).nextValue().toString());
		} catch (JSONException e) {

            LOG.warning("Exception for " + propertyName + " with value "
					+ propertyValue + "\n" + e);
		}
	}

    /**
	 * In case of BLOB data iBATIS returns binary array for BLOB data-type. This
	 * method sets the "binary array" as a content of JSON Object.
	 * 
	 * @param propertyName
	 * @param propertyValue
	 */
	public void setBinaryContent(String propertyName, Object propertyValue) {
		if (propertyValue == null) {
			return;
		}
		try {
			jsonObject.put(propertyName, new SimpleProperty(
					Collections.singletonList(Value.getBinaryValue((byte[]) propertyValue))).nextValue().toString());
		} catch (JSONException e) {
			LOG.warning("Exception for " + propertyName + " with value "
					+ propertyValue + "\n" + e);
		}
	}

    public JSONObject getJsonObject() {
		return jsonObject;
	}


}
