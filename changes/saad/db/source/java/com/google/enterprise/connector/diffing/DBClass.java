package com.google.enterprise.connector.diffing;


import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.base.Function;
import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.Value;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

/**
 * This class implements both the {@link DocumentHandle}
 * and {@link DocumentSnapshot} interfaces. It is backed with a {@link JsonDocument}.
 */
public class DBClass implements DocumentHandle, DocumentSnapshot {
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
			throw new IllegalArgumentException();
		}
		document = new JsonDocument(jo);
		try {
			documentId = Value.getSingleValueString(document,
					SpiConstants.PROPNAME_DOCID);
		} catch (RepositoryException e) {
			throw new IllegalArgumentException();
		}
	}
	@Override
	public Document getDocument() throws RepositoryException {
		// TODO Auto-generated method stub
		return document;
	}

	@Override
	public String getDocumentId() {
		// TODO Auto-generated method stub
		return documentId;
	}

	@Override
	public DocumentHandle getUpdate(DocumentSnapshot onGsa)
			throws RepositoryException {
		// the diffing framework sends in a null to indicate that it hasn't seen
		// this snapshot before. So we return the corresponding Handle (in our
		// case,
		// the same object)
		if (onGsa == null) {
			return this;
		}
		// if the parameter is non-null, then it should be an LdapPerson
		// (it was created via an LdapPersonRepository).
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
