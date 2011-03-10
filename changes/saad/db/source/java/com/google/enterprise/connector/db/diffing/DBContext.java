// Copyright 2011 Google Inc.
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

package com.google.enterprise.connector.db.diffing;

/**
 * This is mostly a data holder class for a particular database connection.
 */
public class DBContext {
    private final String connectionUrl;
    private final String hostname;
    private final String login;
    private final String password;
    private final String dbName;
    private final String driverClassName;

    private String documentURLField;
    private String documentIdField;
    private String baseURL;
    private String lobField;
    private String fetchURLField;
    private String lastModifiedDate;
    private String documentTitle;
    private String extMetadataType;

    private boolean publicFeed = true;

    public DBContext(String connectionUrl, String hostname,
            String driverClassName, String login, String password,
            String dbName, String lastModifiedDate, String documentTitle,
            String documentURLField, String documentIdField, String baseURL,
            String lobField, String fetchURLField, String extMetadataType) {

        this.connectionUrl = connectionUrl;
        this.hostname = hostname;
        this.driverClassName = driverClassName;
        this.login = login;
        this.password = password;
        this.dbName = dbName;

        this.extMetadataType = extMetadataType;
        this.documentURLField = documentURLField;
        this.documentIdField = documentIdField;
        this.baseURL = baseURL;
        this.lobField = lobField;
        this.fetchURLField = fetchURLField;

        this.lastModifiedDate = lastModifiedDate;
        this.documentTitle = documentTitle;

    }

    public void setExtMetadataType(String extMetadataType) {
        this.extMetadataType = extMetadataType;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getHostname() {
        return hostname;
    }

    public String getLogin() {
        return login;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getLastModifiedDate() {
        return lastModifiedDate;
    }

    public String getDocumentTitle() {
        return documentTitle;
    }

    public String getExtMetadataType() {
        return extMetadataType;
    }

    public String getDocumentURLField() {
        return documentURLField;
    }

    public String getDocumentIdField() {
        return documentIdField;
    }

    public String getBaseURL() {
        return baseURL;
    }

    public String getLobField() {
        return lobField;
    }

    public String getFetchURLField() {
        return fetchURLField;
    }

    public boolean isPublicFeed() {
        return publicFeed;
    }

    public void setPublicFeed(boolean publicFeed) {
        this.publicFeed = publicFeed;
    }
}
