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

import java.util.ResourceBundle;

/**
 * This class provide the functionality for getting values from resource bundle.
 * 
 * @author Suresh_Ghuge
 */
public class LanguageResource {

	/**
	 * @param key this is the name of the property you want to access
	 * @return value of the requested property.
	 */
	public static String getPropertyValue(String key) {
		String value = "";
		ResourceBundle bundle = ResourceBundle.getBundle("com.google.enterprise.connector.db.config.DatabaseConfiguration");
		value = bundle.getString(key);
		return value;
	}
}
