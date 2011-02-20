package com.google.enterprise.connector.diffing;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.enterprise.connector.spi.SimpleProperty;
import com.google.enterprise.connector.spi.Value;

public class JsonObjectUtil {

	private static final Logger LOG = Logger.getLogger(
			JsonObjectUtil.class.getName());
	
	/**
	 * Set a property for this document. If propertyValue is null this does
	 * nothing.
	 * 
	 * @param propertyName
	 * @param propertyValue
	 */
	
	private JSONObject jsonObject;
	
	public JsonObjectUtil()
	{
		jsonObject=new JSONObject();
	}
	
	public void setProperty(String propertyName, String propertyValue) {
		if (propertyValue != null) {
			try {
				jsonObject.put(propertyName, new SimpleProperty(Collections.singletonList(Value.getStringValue(propertyValue))).nextValue().toString());
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				LOG.info("JSONException for "+propertyName+" with value "+propertyValue);
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
			return ;
		}
		try {
			jsonObject.put(propertyName, new SimpleProperty(Collections.singletonList(Value.getDateValue(cal))).nextValue().toString());
		} catch (JSONException e) {

			LOG.info("JSONException for "+propertyName+" with value "+propertyValue);
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
			jsonObject.put(propertyName, new SimpleProperty(Collections.singletonList(Value.getBinaryValue((byte[]) propertyValue))).nextValue().toString());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			LOG.info("JSONException for "+propertyName+" with value "+propertyValue);
		}
	}

	public JSONObject getJsonObject() {
		return jsonObject;
	}


}
