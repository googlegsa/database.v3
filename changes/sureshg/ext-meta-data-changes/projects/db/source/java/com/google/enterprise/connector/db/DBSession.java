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


/**
 * This class provides the TraversalManager, AuthenticationManager and the
 * AuthorizationManager
 */
public class DBSession implements Session {
	private final DBClient dbClient;
	private final String xslt;

	/**
	 * @param dbClient
	 * @param xslt
	 */
	public DBSession(DBClient dbClient, String xslt) {
		this.dbClient = dbClient;
		this.xslt = xslt;
	}

	/* @Override */
	public AuthenticationManager getAuthenticationManager() {
		// TODO(meghna): Implement this for GSA.
		throw new UnsupportedOperationException(
				"DBSession does not support getAuthenticationManager");
	}

	/* @Override */
	public AuthorizationManager getAuthorizationManager() {
		// TODO(meghna): Implement this for GSA.
		throw new UnsupportedOperationException(
				"DBSession does not support getAuthorizationManager");
	}

	/**
	 * @return traversal manager for this session.
	 */
	/* @Override */
	public TraversalManager getTraversalManager() {
		return new DBTraversalManager(dbClient, xslt);
	}
}
