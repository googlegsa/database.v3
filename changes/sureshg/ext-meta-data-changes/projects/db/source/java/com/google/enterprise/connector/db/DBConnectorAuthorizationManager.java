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

import com.google.enterprise.connector.spi.AuthenticationIdentity;
import com.google.enterprise.connector.spi.AuthorizationManager;
import com.google.enterprise.connector.spi.AuthorizationResponse;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.AuthorizationResponse.Status;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This class provides an implementation of AuthentorizationManager SPI provided
 * by CM for authorize the search users against Database documents.
 * 
 * @author suresh_ghuge
 */

public class DBConnectorAuthorizationManager implements AuthorizationManager {

	private static final Logger LOG = Logger.getLogger(DBConnectorAuthorizationManager.class.getName());

	private final DBClient dbClient;

	public DBConnectorAuthorizationManager(DBClient dbClient) {
		this.dbClient = dbClient;
	}

	public Collection<AuthorizationResponse> authorizeDocids(
			Collection<String> docIds, AuthenticationIdentity identity)
			throws RepositoryException {

		LOG.info("Documents to be authorized: " + docIds);

		String userName = identity.getUsername();
		Map<String, String> docIdMap = DocIdUtil.getDocIdMap(docIds);
		String docIdString = DocIdUtil.getDocIdString(docIdMap.keySet());
		List<AuthorizationResponse> encodedDocuments = new ArrayList<AuthorizationResponse>();
		List<String> authorizedDocIdList = dbClient.executeAuthZQuery(userName, docIdString);

		/*
		 * Mark Authorization Response status PERMIT for authorized documents.
		 */
		for (String docId : authorizedDocIdList) {
			String encodedDocId = docIdMap.get(docId);
			if (encodedDocId != null) {
				encodedDocuments.add(new AuthorizationResponse(Status.PERMIT,
						encodedDocId));
			}
			docIdMap.remove(docId);
		}

		/*
		 * Mark Authorization Response status DENY for non-authorized documents.
		 */
		Set<String> docIdKeys = docIdMap.keySet();
		for (String docId : docIdKeys) {
			String encodedDocId = docIdMap.get(docId);
			if (encodedDocId != null) {
				encodedDocuments.add(new AuthorizationResponse(Status.DENY,
						encodedDocId));
			}
		}

		return encodedDocuments;
	}
}
