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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class holds the constants required by database connector
 * 
 * @author Suresh_Ghuge
 */
public class DBConnectorConstants {

	// CLOB_COLUMN represents the CLOB column of database table
	public static final String CLOB_COLUMN = "dbconn_clob";
	// BLOB_COLUMN represents the BLOB column of database table
	public static final String BLOB_COLUMN = "dbconn_blob";
	// DOC_COLUMN represents the URL/DOC_ID column of database table for
	// Metadata_URL feed
	public static final String DOC_COLUMN = "dbconn_url";
	// LAST_MOD_COLUMN represents the "Last Modified Date" column of database
	// table
	public static final String LAST_MOD_COLUMN = "dbconn_last_mod";
	// MIME_TYPE_COLUMN represents the "MIME Type" column of database table
	public static final String MIME_TYPE_COLUMN = "mime_type";
	// LOB_URL represents the URL for fetching the BLOB/CLOB document stored in
	// the database table
	public static final String LOB_URL = "dbconn_lob_url";

	// TITLE_COLUMN represents the "Document Title" column of database table
	public static final String TITLE_COLUMN = "dbconn_title";

	/**
	 * this method returns the list of column which needs to skip while indexing
	 * as they are not part of metadata or they are already considered for
	 * indexing. For example document_id column, MIME type column, URL columns.
	 * 
	 * @return list of columns needs to be skip during indexing.
	 */
	static List<String> getSkipsForMetaColumns() {
		List<String> skipColumnList = new ArrayList<String>();
		skipColumnList.addAll(Arrays.asList(CLOB_COLUMN, BLOB_COLUMN, DOC_COLUMN, LAST_MOD_COLUMN, MIME_TYPE_COLUMN, LOB_URL, TITLE_COLUMN));
		return skipColumnList;
	}
}
