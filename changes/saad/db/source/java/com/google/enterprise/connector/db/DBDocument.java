// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.Property;
import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.SkippedDocumentException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.spi.TraversalContext;
import com.google.enterprise.connector.spi.Value;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * An implementation of Document for database records. Each row in the database
 * represents a {@link DBDocument}.
 */
public class DBDocument implements Document {
	private static final Logger LOG = Logger.getLogger(DBDocument.class.getName());
	private final Map<String, List<Value>> properties = new HashMap<String, List<Value>>();
	public static final String ROW_CHECKSUM = "dbconnector:checksum";
	private TraversalContext traversalContext;

	

	/**
	 * Constructs a document with no properties.
	 */
	public DBDocument(TraversalContext traversalContext) {
		this.traversalContext=traversalContext;
	}

	/* @Override */
	public Property findProperty(String name) throws SkippedDocumentException {
		List<Value> property = properties.get(name);
		if (name.equals(SpiConstants.PROPNAME_MIMETYPE)) {
		
			filterMimeType();
		} else if (name.equals(SpiConstants.PROPNAME_CONTENT)) {
			int val=filterMimeType();
			if(val==0)
				property=null;
		}

		return (property == null) ? null : new SimpleProperty(property);
	}

	/**
	 * Returns all the property names.
	 */
	/* @Override */
	public Set<String> getPropertyNames() {
		return Collections.unmodifiableSet(properties.keySet());
	}

	/**
	 * Set a property for this document. If propertyValue is null this does
	 * nothing.
	 * 
	 * @param propertyName
	 * @param propertyValue
	 */
	public void setProperty(String propertyName, String propertyValue) {
		if (propertyValue != null) {
			properties.put(propertyName, Collections.singletonList(Value.getStringValue(propertyValue)));
		}
	}

	/**
	 * This method adds the last modified date property to the DB Document
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
		properties.put(propertyName, Collections.singletonList(Value.getDateValue(cal)));
	}

	/**
	 * In case of BLOB data iBATIS returns binary array for BLOB data-type. This
	 * method sets the "binary array" as a content of DB Document.
	 * 
	 * @param propertyName
	 * @param propertyValue
	 */
	public void setBinaryContent(String propertyName, Object propertyValue) {
		if (propertyValue == null) {
			return;
		}
		properties.put(propertyName, Collections.singletonList(Value.getBinaryValue((byte[]) propertyValue)));
	}

	
	/**
	 * This method will analyze the value of "Mime Type Support Level". If value
	 * is 0 warning message will be logged and value is negative then
	 * SkippedDocumentException will be thrown.
	 * 
	 * @throws SkippedDocumentException
	 */
	private int filterMimeType() throws SkippedDocumentException {

		String docId = properties.get(SpiConstants.PROPNAME_DOCID).iterator().next().toString();
		List<Value> mimeTypeProperty = properties.get(SpiConstants.PROPNAME_MIMETYPE);
	
		String mimeType = null;
		if (mimeTypeProperty != null) {	
			mimeType = mimeTypeProperty.iterator().next().toString();
		}
		mimeType = mimeType == null ? "'no mime'" : mimeType;
		
		int mimeTypeSupportLevel = this.traversalContext.mimeTypeSupportLevel(mimeType);
		if (mimeTypeSupportLevel == 0) {
			try
			{
			List<Value> content=properties.remove(SpiConstants.PROPNAME_CONTENT);
			LOG.warning("Skipping the contents"+content+" with docId : " + docId
					+ " as content mime type " + mimeType + " is not supported");

			
			return 0;
			}
			catch(Exception e)
			{
					LOG.warning("Cannot make the contents null");
			}
		//	LOG.warning("Skipping the document with docId : " + docId
			//		+ " as content mime type " + mimeType + " is not supported");

		} else if (mimeTypeSupportLevel < 0) {
			String msg = new StringBuilder(
					"Skipping the document with docId : ").append(docId).append(" as the mime type ").append(mimeType).append(" is in the 'ignored' mimetypes list").toString();

			LOG.warning(msg);
			throw new SkippedDocumentException(msg);
		}
		return-1;
	}
	
	
}
