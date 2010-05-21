//Copyright 2010 Google Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.google.enterprise.connector.db;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DocIdUtil {
	private static final Logger LOG = Logger.getLogger(DocIdUtil.class.getName());
	public static final String PRIMARY_KEYS_SEPARATOR = ",";

	/**
	 * This method decode the Base64 encoded doc ids and returns the comma
	 * separated String of document ids.
	 * 
	 * @param docIds
	 * @return comma separated list of doc ids.
	 */
	public static String getDocIdString(Collection<String> docIds) {
		StringBuilder docIdString = new StringBuilder("");

		for (String docId : docIds) {
			docIdString.append("'" + docId + "'" + ",");
		}

		return docIdString.substring(0, docIdString.length() - 1);
	}

	/**
	 * This method creates and returns a map of decoded and encoded docIds. Here
	 * decoded docIds are used as keys and encoded docIds are used as values.
	 * 
	 * @param docIds
	 * @return map of decoded and encoded doc Ids
	 */
	public static Map<String, String> getDocIdMap(Collection<String> docIds) {
		Map<String, String> docIdMap = new HashMap<String, String>();
		for (String docId : docIds) {
			try {
				docIdMap.put(decodeBase64String(docId), docId);
			} catch (IOException e) {
				LOG.warning("Exception occured while decoding doc Id : "
						+ docId + e.toString());
			}
		}
		return docIdMap;
	}

	/**
	 * This method decode the Base64 encoded input string.
	 * 
	 * @param inputString
	 * @return
	 * @throws IOException
	 */

	public static String decodeBase64String(String inputString)
			throws IOException {
		byte[] docId = new BASE64Decoder().decodeBuffer(inputString);
		return new String(docId);
	}

	public static String getBase64EncodedString(String inputString) {
		String base64EncodedString = new BASE64Encoder().encode(inputString.getBytes());

		return base64EncodedString;
	}

	/**
	 * Generates the docId for a DB row. Base 64 encode comma separated key
	 * values are used as document id. For example, if the primary keys are id
	 * and lastName and their corresponding values are 1 and last_01, then the
	 * docId would be the BASE64 encoded of "1,last_01" i.e "MSxmaXJzdF8wMQ==".
	 * 
	 * @return docId Base 64 encoded values of comma separated primary key
	 *         columns.
	 */

	public static String generateDocId(String[] primaryKeys,
			Map<String, Object> row) throws DBException {

		StringBuilder docIdString = new StringBuilder();
		if (row != null && (primaryKeys != null && primaryKeys.length > 0)) {
			Set<String> keySet = row.keySet();

			for (String primaryKey : primaryKeys) {
				/*
				 * Primary key value is mapped to the value of key of map row
				 * before getting record. We need to do this because GSA admin
				 * may entered primary key value which differed in case from
				 * column name.
				 */

				for (String key : keySet) {
					if (primaryKey.equalsIgnoreCase(key)) {
						primaryKey = key;
						break;
					}
				}
				if (!keySet.contains(primaryKey)) {
					String msg = "Primary Key does not match with any of the coulmn names";
					LOG.info(msg);
					throw new DBException(msg);
				}
				Object keyValue = row.get(primaryKey);
				if (null != keyValue) {
					docIdString.append(keyValue.toString()
							+ PRIMARY_KEYS_SEPARATOR);
				}
			}
		} else {
			String msg = "";
			if (row == null && (primaryKeys == null || primaryKeys.length == 0)) {
				msg = "row is null and primary key array is empty";
			} else if (row == null) {
				msg = "hash map row is null";
			} else {
				msg = "primary key array is empty or null";
			}
			LOG.info(msg);
			throw new DBException(msg);
		}
		/*
		 * substring docId String to remove extra "," at the end on docId
		 * String.
		 */
		docIdString.deleteCharAt(docIdString.length() - 1);
		// encode doc Id.
		String encodedDocId = getBase64EncodedString(docIdString.toString());
		return encodedDocId;
	}
}
