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


import com.google.common.base.Function;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Logger;

/**
 * This class implements both the {@link DocumentHandle}
 * and {@link DocumentSnapshot} interfaces. It is backed with a {@link JsonDocument}.
 */
public class DBClass implements DocumentHandle, DocumentSnapshot {
	private static final Logger LOG = Logger.getLogger(DBClass.class.getName());
	private final JsonDocument document;
	private final String documentId;
	private final String jsonString;

    public DBClass(JsonDocument jsonDoc) {
		document = jsonDoc;
		documentId = document.getDocumentId();
		jsonString = document.toJson();
	}

    public static Function<JsonDocument, DBClass> factoryFunction = new Function<JsonDocument, DBClass>() {
		/* @Override */
		public DBClass apply(JsonDocument jsonDoc) {
			return new DBClass(jsonDoc);
		}
	};

    public DBClass(String jsonString) {
		// This is implemented by saving the supplied jsonString then making a
		// JSONObject
		this.jsonString = jsonString;
		JSONObject jo;
		try {
			jo = new JSONObject(jsonString);
		} catch (JSONException e) {

            LOG.warning("JSONException thrown while creating JSONObject from string"
					+ jsonString);
			throw new IllegalArgumentException();

        }
		document = new JsonDocument(jo);
		try {
			documentId = Value.getSingleValueString(document, SpiConstants.PROPNAME_DOCID);
		} catch (RepositoryException e) {
			LOG.warning("Repository Exception thrown while extracting docId for Document"
					+ document);
			// Thrown to indicate an inappropriate argument has been passed to
			// Value.getSingleValueString() method.
			throw new IllegalArgumentException();
		}
	}

    /* @Override */
	public Document getDocument() throws RepositoryException {
		return document;
	}

    /* @Override */
	public String getDocumentId() {
		return documentId;
	}

    /**
	 * Returns a {@link DocumentHandle} for updating the referenced document on
	 * the GSA or null if the document on the GSA does not need updating.
	 * 
	 * @throws RepositoryException
	 */
	/* @Override */
	public DocumentHandle getUpdate(DocumentSnapshot onGsa)
			throws RepositoryException {
		// the diffing framework sends in a null to indicate that it hasn't seen
		// this snapshot before. So we return the corresponding Handle (in our
		// case,
		// the same object)
		if (onGsa == null) {
			return this;
		}
		// if the parameter is non-null, then it should be an DBClass
		// (it was created via an DBClassRepository).
		if (!(onGsa instanceof DBClass)) {
			throw new IllegalArgumentException();
		}
		DBClass p = DBClass.class.cast(onGsa);
		// we just assume that if the serialized form is the same, then nothing
		// has changed.
		if (this.jsonString.equals(p.toString())) {
			// null return tells the diffing framework to do nothing
			return null;
		}
		// Something has changed, so return the corresponding handle
		return this;
	}

    public String toString() {
		return jsonString;
	}
}
