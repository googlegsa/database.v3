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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SimpleDocument;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A simple {@link Document} implementation created from a {@link JSONObject}.
 */
public class JsonDocument extends SimpleDocument {

	private static final Logger LOG = Logger.getLogger(JsonDocument.class.getName());

	private final String jsonString;
	private final String objectId;

	/**
	 * Constructor used by @link Util.java for creating a JsonDocument object
	 * used by @link RepositoryHandler.java for building a collection over JsonDocument
	 */
	public JsonDocument(Map<String, List<Value>> properties,
			JSONObject jsonObject) {
		super(properties);
		jsonString = jsonObject.toString();
		try {
			objectId = Value.getSingleValueString(this, SpiConstants.PROPNAME_DOCID);
		} catch (RepositoryException e) {
			throw new IllegalStateException("Internal consistency error", e);
		}
		if (objectId == null) {
			throw new IllegalArgumentException(
					"Unable to parse for docID from the JSON string:"
					+ jsonString);
		}

	}

	/**
	 * Constructor used by DBClass for creating JsonDocument for change Detection purpose.
	 */

	public JsonDocument(JSONObject jsonObject) {
		super(buildJsonProperties(jsonObject));
		jsonString = jsonObject.toString();
		try {
			objectId = Value.getSingleValueString(this, SpiConstants.PROPNAME_DOCID);
		} catch (RepositoryException e) {
			throw new IllegalStateException("Internal consistency error", e);
		}
		if (objectId == null) {
			throw new IllegalArgumentException(
					"Unable to parse for docID from the JSON string:"
					+ jsonString);
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
	 * creating a Map<String,List<Value>> used by the superclass(@link
	 * SimpleDocument) constructor and hence creating a JsonDocument Object.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, List<Value>> buildJsonProperties(JSONObject jo) {
		ImmutableMap.Builder<String, List<Value>> mapBuilder = new ImmutableMap.Builder<String, List<Value>>();
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
	 * A class level method for extracting attributes from JSONObject object
	 * used by buildJsonProperties.
	 */
	private static void extractAttributes(JSONObject jo,
			ImmutableMap.Builder<String, List<Value>> mapBuilder, String key)
	throws IllegalAccessError {
		String ja;
		try {
			ja = (String) jo.get(key);
		} catch (JSONException e) {
			LOG.warning("Exception thrown while extracting key: " + key + "\n"
					+ e);
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
		mapBuilder.put(SpiConstants.PROPNAME_DOCID, ImmutableList.of(Value.getStringValue(docid)));

	}


}
