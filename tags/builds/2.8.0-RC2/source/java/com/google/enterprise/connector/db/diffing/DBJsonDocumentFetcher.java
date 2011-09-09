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

import java.util.Iterator;
import java.util.logging.Logger;


/* Uses an RepositoryHandlerIterator to implement JsonDocumentFetcher */
public class DBJsonDocumentFetcher implements JsonDocumentFetcher{
    private static Logger LOG = Logger.getLogger(DBJsonDocumentFetcher.class.getName());
    private RepositoryHandlerIterator repositoryHandlerIterator;

	public DBJsonDocumentFetcher(RepositoryHandler repositoryHandler) {
        this.repositoryHandlerIterator = new RepositoryHandlerIterator(
                repositoryHandler);
    }

    /* @Override */
    public Iterator<JsonDocument> iterator() {

        return repositoryHandlerIterator;
    }



}
