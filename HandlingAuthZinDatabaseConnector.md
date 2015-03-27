# Introduction #
This document gives an idea of the way Database Connector handles the security related tasks pertaining to the documents it sends out to the GSA.

When a user fires a search, GSA needs to filter out the search results based on the user’s rights. This is known as Authorization. Though, before delegating the authorization job to the connector, GSA needs to ensure that the search user is a valid user. This is known as Authentication.


### User Authentication ###
Database connector does not support Authentication of the user. But GSA admin can use other AuthN schemes (Kerberos, LDAP) for authentication of user.

### Document Authorization ###
Database connector would handle authorization by default only for those documents which are sent as a content feed to GSA. If the database connector is configured for metadata-and-URL feeds, using either a base URL and docId OR a URL field, the authorization would be done by HEAD requests. Both of these defaults can be configured using flexible authorization.

For example. there might be cases where customers have a web-portal to handle authorization of the documents. The link for the documents and related metadata is stored in a database.

As per the Connector SPI, the Database connector will get a collection of DocIDs to be authorized and an AuthenticationIdentity (the user identity) . The document IDs are sufficient enough to identify the document in Database table and do authorization. These IDs are ones which connector sent during the traversal. Database connector uses the following pattern for constructing document IDs:

**`[`value of primary key column1], `[`value of primary key column2], ...    ,`[`value of primary key columnN]**

Here DocId is the BASE64 encoded, comma separated values of primary key column(s). Column values of the primary key should appear in same order which user has used while configuring the database connector. For example. if you have configured the database connector with "report\_id" and "report\_title" as primary key columns then DocId for report\_id "23" and report\_title "Finance", the DocId will be the Base64 encoded value of "23,Finance".

While configuring Database Connector, connector Admin should provide valid SQL query for authorizing collection of documents (AuthZ Query). The "AuthZ Query" should have two placeholders , one for user-name and other is for taking collection of DocIds to be authorized. This query must used "IN clause" to take DocIds to be authorized. There should be mapping between user-name and records in Database system.  The authZ query should return result as a comma separated list of primary key column values which are authorized for the given user


Below example explain how to write "AuthZ Query" for "USER\_REPORT\_MAP" table. This table maintain the mapping between user and reports user is allowed to view.

Here ReportId and ReportTitle are primary key columns in REPORT Table.


SELECT CONCAT(ReportId ,",", ReportTitle) FROM USER\_REPORT\_MAP Where username=#username# AND CONCAT(ReportId ,",", ReportTitle) IN ($docIds$)


The above query may produce output for some user "alvin\_2" as show below.

'2,feb'
'1,mar'


Below screen shot shows AuthZ Query field on Database Connector configuration form:

AuthZ Query is optional field on database connector configuration form. If user provides "AuthZ query" ,all feeds to the GSA  will be sent as private feed otherwise public feed will be sent.

## Role based authorization ##

There can be scenarios where documents are accessible by users with a particular role. In this scenario it is assumed that one or more tables maintain the mapping between role and documents to be authorized. Connector user can provide authZ query such that it accept documents to be authorized and  the user-name . AuthZ query should be able to find out the role of the given user and it should return the authorized documents.

The above example can be modified to demonstrate this scenario. Consider RoleMap table maintain the mapping between documents and permitted role .
We can get role of user from the Users table and then we can get the list of authorized doc ids from role.


SELECT CONCAT(ReportId ,",", ReportTitle) FROM RoleMap WHERE Permited\_role = (SELECT role FROM users WHERE username= #username#)  AND CONCAT(ReportId ,",", ReportTitle)  IN ($docIds$)

This query will return the list of authorized documents.

'1,mar'
'2,feb'

Connector admin can provide authZ query as per database design. This query should have placeholders for user-name and list of documents.