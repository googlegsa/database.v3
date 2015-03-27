# Introduction #
This document has a list of troubleshooting tips and FAQ to quickly identify if connector has been configured correctly and is discovering & crawling as intended.

# Error Messages #
This section describes some commonly encountered error messages and their likely solutions.
### Commmon to Both Content and Metadata-and-URL Feed Mode ###

*** Hostname should be fully qualified.**
> You must provide fully qualified name of connector host. For example **connector-host.domain**
*** Could not connect to the database.**
> You see this error message when user provided Database Connection URL is not valid or  given user does not have sufficient privileges. Check [here](http://code.google.com/p/google-enterprise-connector-database/wiki/DBConnectorConfigurationDetails) for more details.
*** One or more primary key values are invalid.**
> You see this error message when user provided primary keys are not found in Resultset. User must provide primary keys which are there in Resultset.
*** Invalid database driver class.**
> You see this message when provided driver class is invalid. User must provide fully qualified name of the JDBC driver class.
*** Invalid SQL query.**
> You see this error message when user provide invalid SQL query or SQL query is not of type SELECT. User must provide valid SQL SELECT query.
*** Missing configuration attributes**
> You see this error message when user does not provide value for  BASE URL or BLOB/CLOB Field when respective mode of external metadata feature is selected

*** Required fields are missing**
> Fields marked with an asterisk (`*`) on the Configuring Connector Instances form are required. You must provide appropriate values for these fields.
*** One or more column names are invalid**
> This error message is displayed when user provides wrong column names.
*** Invalid AuthZ Query**
> This error message is displayed when user enter invalid authZ query. AuthZ Query should have placeholder for docIds and username and it should be valid SELECT query.

# Diagnosing the Connector Logs #
This section details some of the important log messages that are written into the connector’s log:

| **Log Message** | **Description** | **Logging Level** |
|:----------------|:----------------|:------------------|
|startTraversal / resumeTraversal | A new / incremental crawl has begun. |INFO |
| N document(s) to be fed to GSA | Connector is ready to send N number of documents to Connector Manager. | INFO|
| M document(s) are marked for delete feed | Connector has marked M number of documents for delete| INFO |
| DB Connector is running in content feed mode for text data| Connector is running in Content feed mode and crawling text data | INFO |
|  DB Connector is running in Content Feed Mode for BLOB/CLOB data | Connector is running in Content feed mode and crawling BLOB/CLOB data| INFO |
|  DB Connector is running in External Metadata feed mode with complete document URL | Connector is running in Metadata and URL mode and document URL is the value of Document URL field| INFO|
| DB Connector is running in External Metadata feed mode with Base URL and document ID | Connector is running in Metadata and URL mode and document URL is built using Base URL and value of document id field| INFO |
| Crawl cycle of database `<`database name`>` is completed at: `<`date-time`>`. Total `<`recordCount`>` records are crawled during this crawl cycle | This log message is logged at the end of every crawl cycle | INFO |
| Size of the document `<`docId`>` is larger than supported| Size of document with docid `<`docId`>` is larger than connector manager supports| WARNING|
| Skipping the document with docId : `<`docId`>` as content mime type `<`mimeType`>` is not supported. | Mimetype `<`mimeType`>` of document with docId `<`docId`>` is not supported| WARNING |
|Unable to connect to the database | Database connector is unable to connect to the database. This could happened because database server has shut down or network failure.| WARNING|
|Could not execute AuthZ query on the database|User provided AuthZ Query is not  correct syntactically|WARNING|
|User: `<`user name`>` is authorized for : `<`list of docuement Ids`>` and not authorized for document `<`list of document Ids`>`| User is is allowed to view  <list of document Ids> and denied for  <list of document Ids> |INFO |
|Traversal resumed at `<`date-time`>` from checkpoint <checkpoint string>|Traversal has resumed at `<`date-time`>` from previous checkpoint| INFO |
|Could not load state. Starting all over again|Connector is running first time or state file is deleted intentionally to force re-crawling of entire data.| WARNING|


# Frequently Asked Questions - FAQ #
This section lists some of the most commonly asked questions.

**Q. I cannot register the Connector Manager on GSA. What should I do?**

You can test that the connector manager URL is valid and is running by typing the URL in a browser: `http://<localhost>:<tomcat_port>/connector-manager` on the machine that has the Connector Manager and connector installed on it You will get an informative text displaying the connector manager version. You should see something like:
```
<CmResponse>
        <Info>Google Search Appliance Connector Manager 3.2.2-RC1 (build 3276 3.2.2-RC1 October 29 2013); Oracle Corporation Java HotSpot(TM) 64-Bit Server VM 1.7.0_51; Windows Server 2012 6.2 (amd64)</Info> 
        <StatusId>0</StatusId> 
</CmResponse>
```
If you see the above response and GSA is still unable to register the Connector Manager, you need to check the network settings between your GSA and the Connector Manager host. If you do not see the above response, then please check that the Connector Manager host is reachable and it is running.

**Q. How can I track the feeds that connector sends to the GSA?**

Set the feedLoggingLevel property to ALL in the applicationContext.properties found under $CATALINA\_HOME/webapps/connector-manager/WEB-INF/. Restart the connector and let it run for some time. Check out the google-connectors.feed log files generated under $CATALINA\_HOME/logs/ folder.

**Q. How can I change the log level so that only the relevant log messages are generated?**

Go to $CATALINA\_HOME/webapps/connector-manager/WEB-INF/classes. Open logging.properties and change the log level(s) to the required level(s)

**Q. Is it possible for a single connector to send feed to more then one GSA?**
> No. You have to register a connector manager and create connector instances under this, on each individual GSA.

**Q. Can I create multiple connector instances with the same name?**
> No.

**Q. Does connector run in an incremental mode, so that the changes done in database table(s) are reflected during search?**
> Yes. Connector sends new feeds for the documents which are modified.

**Q. How can I search metadata for a document?**
> Use inmeta search for this. For details, refer to http://code.google.com/apis/searchappliance/documentation/52/xml_reference.html#inmeta_filter

**Q: Do I need to restart the connector service each time I modify the connector configuration?**
> No

**Q: Do I need to restart the connector service each time I modify the connectorInstance.xml?**
> Yes

**Q. I can’t get the connector to re-crawl using the 'Reset' feature. Is there any other way of forcing a re-crawl?**
> The manual steps to force a re-crawl of the connector.
  1. On the connector host, navigate to the location of the connector state file.

  * On Windows, this is <Installation Location>\Tomcat\webapps\connector-manager\WEB-INF\connectors\DBConnector\Database Connector Instance Name\.
  * On Linux, this is <Installation Location>/Tomcat/webapps/connector-manager/WEB-INF/connectors/DBCOnnector/Database Connector Instance Name/
  1. Delete the file DBConnector\_state.xml .
  1. Restart the Database Connector.
> The connector traverses the content again and generates new feeds.

**Q. How do I change the port on which the Connector Manager is running?**
> Go to `<`Installation Directory`>`/Tomcat/conf and edit the server.xml file as follows: Find: `<`Connector port="`<`portNo`>`". Here replace the `<`portNo`>` with the port configured during initial installation. Specify a new port value for 'port' attribute and restart Connector service.

**Q. Can I restore a connector instances in case it has been deleted by mistake?**
> No. Though, you can always create a new connector instance with the same name and same configuration details as of the deleted connector instance. This will serve the same purpose except the state information is lost. In that case, connector will re-crawl the whole database table(s) again.

**Q. I have many tables in database that I want crawl. Will single connector instance be able to crawl all these?**
> Yes. You can crawl multiple tables using JOIN or by configuring separate connector instance for each database table.

**Q. Does connector keep track of the deleted rows so that they are removed from the GSA’s index?**
> Yes. Connector sends delete feeds for such documents. GSA, than removes all such documents and their contents from its index.

**Q. I have last modified date of the documents stored in database table. Can I specify last modified date while configuring database connector?**
> Yes. You can specify last modified date of the document in "Last Modified Date Field" of Database Connector configuration form.

**Q. I have document modified stored in database table. Can I specify document title field while configuring database connector?**
> Yes. You can specify last document title of the document in "Document Title Field" of Database Connector configuration form.

**Q. Can I configure Database connector to handle authentication?**
> No. Current version of Database connector does not support authentication.


**Q. Does DB connector support ssl for database connections?**
> No. Current version of Database connector does not support ssl for database connections.

**Q. Can I configure Database connector handle authorization?**
> Yes. You can configure Database connector to handle authorization. Click [here](http://code.google.com/p/google-enterprise-connector-database/wiki/HandlingAuthZinDatabaseConnector) for more details.

**Q. Can I configure Database connector to call stored procedures ?**
> No. Current version of Database connector does not support call to stored procedures.

### External Metadata Indexing Using Database Connector ###
**Q. Can I configure Database Connector to crawl and index external metadata?**
> Yes. You can configure Database Connector to crawl and index external metadata. Click [here](http://code.google.com/p/google-enterprise-connector-database/wiki/ExternalMetadataIndexingUsingDBConnector) for more details.

**Q. I have document URL and associated metadata stored in database table(s). Can I configure Database Connector to crawl and index this data in Metadata and URL mode  ?**
> Yes. You can configure Database Connector to send Metadata and URL feeds to GSA for URL and metadata stored in database table(s). Click [here](http://code.google.com/p/google-enterprise-connector-database/wiki/ExternalMetadataIndexingUsingDBConnector) for more details.

**Q. I have BLOB/CLOB data stored in database. Can I configure Database Connector to crawl BLOB/CLOB data?**
> Yes. You can configure Database Connector to crawl and index BLOB or CLOB data. Click [here](http://code.google.com/p/google-enterprise-connector-database/wiki/ExternalMetadataIndexingUsingDBConnector) for more details.

**Q. Can we use stylesheet to let admin decide which fields to be sent in as metadata using the "meta" tag?**
> No. Current version of Database connector does not support this feature.