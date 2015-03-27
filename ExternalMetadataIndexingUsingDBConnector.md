# Introduction #

---

This document covers the use-cases and implementation details of "external metadata indexing using Database Connector". There can be scenarios where metadata is not stored in primary document but in database table(s). Database connector can be configured to index metadata stored in database tables. When you index external metadata, it is searchable in the same way that other metadata is searchable.

There are three scenarios for indexing external metadata that is stored in a database, depending on how your primary document is referenced and stored.  For each of these scenarios a meta name for each column(field) is metadata-key and metadata-value is value of that column. In all three scenarios primary key column(s) are not considered for indexing.

## Scenario 1: ##

---

In this scenario a valid URL of the primary document is stored in a single column in the database table. Other columns hold the metadata associated with the primary document.

In this scenario, the Database Connector queries the database for data, then submits a metada-URL feed . The Database Connector crawls the set of records that is defined by the crawl query. The URL to the primary document is extracted from each record( from the field defined as the URL field). Name of the each column is used as metadata-key and metadata-value is the value of that column. Primary key columns are not considered as external metadata.

**Where to specify URL column(field)?**

New configuration field **"Document URL Field"** has introduced on Database Connector configuration form, connector admin must enter the name of the column which holds the URL of documents to be crawled and indexed in this field. Please see below screen shot of DB Connector configuration form for **"Document URL Field"**.

## Scenario 2: ##

---

This scenario is very similar to the first one, except the URL is constructed from a base URL and a document ID.
If your external metadata is stored in a relational database and the URLs that reference primary documents can be constructed by combining a base URL string and a database field. The database field usually represents a unique document ID number that, when inserted into a base URL string, references a specific document on a web server. For example

`http://my-host:6502/getdoc?action=get&docid=4662118437`

In the example, the highlighted number represents a unique document ID stored in database field. You can configure the Database Connector to crawl the metadata and construct URLs that refers primary documents by combining value of document id field and base URL.

For above example user must enter `"http://my-host:6502/getdoc?action=get&docid=" ` as base url.


**Where to specify Base URL and Document field  ?:**

New fields **"Base URL"** and **"Document ID Field"** has introduced on Database Connector configuration form where user can enter the Base URL string and name of the document id column.

## Scenario 3: ##

---

In this scenario external metadata is stored in a relational database and the primary document is also stored in the database as a Large Object(BLOB/CLOB).

**Where to specify Document ID column(field)?**

A new field **"BLOB/CLOB Field"** has introduced on Database Connector configuration form where user can enter the name of field that holds the primary document(BLOB/CLOB).
If database table maintain the column for fetching this BLOB/CLOB data, user can specify the name of that column in "Fetch URL Field".

**MIME type of the BLOB data?**

MIME type of the BLOB data is determined programatically. MimeUtil library is used to determine the MIME type of the BLOB data.  **"text/html"** is used as MIME type for CLOB data.

**"Document Title" and "Last Modified Date" fields**

---

**Where to specify**"Last Modified Date"**of the document for above three scenarios?**

If database maintains the last modified date of the document in one of its column. User can enter the name of this field in **"Last Modified Date Field"** field of database connector configuration form.

**Where to specify**"Title"**of the document for above three scenarios?**

If database maintains the **"Title"** of the document in one of its column. User can enter the name of this field in **"Document Title Field"** field of database connector configuration form.

Note: Above all fields are not required field.