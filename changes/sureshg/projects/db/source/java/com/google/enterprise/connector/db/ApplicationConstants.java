//Copyright 2009 Google Inc.
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

/**
 * This class holds the constants required by database connector
 * 
 * @author Suresh_Ghuge
 */
public class ApplicationConstants {

	// represent the CLOB column of database table
	public static final String CLOB_COLUMN = "dbconn_clob";
	// represent the BLOB column of database table
	public static final String BLOB_COLUMN = "dbconn_blob";
	// represent the URL or DOC ID column of database table
	public static final String URL_COLUMN = "dbconn_url";
	// represent the "Last Modified Date" column of database table
	public static final String LAST_MOD_COLUMN = "dbconn_last_mod";
	// represent the "MIME Type" column of database table
	public static final String MIME_TYPE_COLUMN = "mime_type";
}
