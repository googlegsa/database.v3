# Introduction #
This document will provide you the details of various fields of configuration form for the Google
Search Appliance Connector for Databases.

# Prerequisites #

  * Database connector is installed and running.


# Configuration form fields details #
  * **Username:**Username is a required field and the value of this field must be the username of Database server user.  This field value is used to connect to the database. Please note that Windows authentication used in MS SQL server is not currently supported by the connector.

  * **Password:**Password is a mandatory field, but in case database user password is empty string then user can keep this field blank. This field value is used to establish connection with the database.

  * **JDBC Connection URL:**This is a required field. Valid connection URL must be supplied to connect to the database. Database server should be accessible from remote host while configuring database connector. Connection URL must follow below format.

> Protocol://db-server: port/DB
> For example "jdbc:mysql://10.88.45.40:3306/MySQL" is JDBC Connection URL for connecting to the database “MySQL” which is running on server “10.88.45.40” and port 3306 using jdbc:mysql protocol.

> If your database and connector are running on different servers, then database should be accessible from connector host for supplied user-name and password. For example, if "user1" and "password" are the user-name and password of database user respectively, then user "user1" should have remote access to the database. Database admin can grant remote access to database using below SQL script(this example is for MySQL database).

> GRANT ALL PRIVILEGES ON **.** TO user1@'connector-host' IDENTIFIED BY 'user1-password'

> Note : If you configuring DB2 database with with BLOB/CLOB feature then you must append ":driverType=4;fullyMaterializeLobData=true;fullyMaterializeInputStreams=true;progressiveStreaming=2;progresssiveLocators=2;" at the end of Connection URL. The final URL will look as below

> jdbc:db2://db2-server:port/testDB:driverType=4;fullyMaterializeLobData=true;fullyMaterializeInputStreams=true;progressiveStreaming=2;progresssiveLocators=2;

  * **Database Name:**Database name is a required field. User must supply value of database name. This field value is used in xslt , display URL and xml representation of a row.

  * **Connector Hostname:**Hostname is a required field. Fully qualified Connector hostname should be provided. For Example hostname.domain. Hostname is used in display URL in the search results.

  * **JDBC Driver Classname:**This is a required field. Fully qualified driver class name  should be provided without .class extension. The value of this field is used by iBatis framework to load JDBC driver class. For example: for My SQL database user must type “com.mysql.jdbc.Driver” in JDBC Driver Classname field.

> For details of different JDBC drivers for various databases you can refer to: http://www.dbvis.com/products/dbvis/doc/supports.jsp

  * **SQL Query:**SQL query is a required field. User must provide valid SELECT SQL query. iBatis framework executes this SQL query for fetching records from database while crawling.

> Data from multiple tables can be fetched using joins.
> Example:
> SQL query for crawling data from single table employee with column empid , first\_name, last\_name, manager and deptno: SELECT empid , first\_name, last\_name, manager , deptno FROM employee.

> Data from multiple tables can be crawled using JOIN or UNION. For example SQL query for crawling data from multiple tables(suppliers and orders): SELECT suppliers.supplier\_id, suppliers.supplier\_name, orders.order\_date  FROM suppliers, orders WHERE suppliers.supplier\_id = orders.supplier\_id;


  * **Primary Keys:**This is a required field. User must provide primary key column name for given SQL crawl query. If table is using composite primary key, then the columns forming primary key should be given with comma separated value. If SQL query contain aliases for column names, then alias should be used as primary key value. Primary key field is not case sensitive. Value of this field is used in building DOC\_ID.

  * **Stylesheet for serving results:**This is not a required field. User can provide valid xslt for customized look and feel. If xslt value is not supplied, default Stylesheet will be used

> See below example of xslt for an employee table. Here firstName , lastName, email and id are column names and DB\_Name is database name.
```
<?xml version="1.0" encoding="UTF-8"?><xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:template match="/">
  <html>
  <body>
  <xsl:for-each select="DB_Name">
  <title><xsl:value-of select="title"/></title>
  </xsl:for-each>
    <table border="1">
      <tr bgcolor="#9acd32">
        <th>First Name</th>
        <th>Last Name</th>
        <th>Email</th>
        <th>Id</th>
      </tr>
      <xsl:for-each select="DB_Name">
      <tr>
        <td><xsl:value-of select="firstName"/></td>
        <td><xsl:value-of select="lastName"/></td>
        <td><xsl:value-of select="email"/></td>
        <td><xsl:value-of select="id"/></td>
      </tr>
      </xsl:for-each>
    </table>
  </body>
  </html>
</xsl:template>
</xsl:stylesheet>
```
  * **Last Modified Date Field:**This is an optional field. If database maintains the last modified date of the document in one of its column. User can enter the name of this field in "Last Modified Date Field"field.
  * **Document Title Field:**This is an optional field. If database maintains the "Title" of the document in one of its column. User can enter the name of this field in "Document Title Field" field.


### External Metadata Indexing Fields ###

  * **Document URL Field:**This is an optional field. User can select this option of external metadata indexing by selecting corresponding radio button. If database maintains the absolute URL of the primary document in one of the column of database table, user can mention the same in this field. As this is "Metadata-URL" feed, user must enter valid URL pattern in include URLs of GSA through admin console.
  * **Document ID Field:**This is an optional field. If database maintains the document identity in one of the columns of database table, user can select this option of external metadata indexing by selecting corresponding radio button. In this case user must enter valid column name in this field.
  * **Base URL:**This field becomes a mandatory field when user selects "Document ID Field" radio button. User must enter valid value for this field. Absolute URL of the document is constructed by the connector using "Base URL" field value combined with document identity.
  * **BLOB/CLOB Field :**This field is an optional field. If database table maintains the primary document stored in one of the columns as BLOB/CLOB data, user can mention the same column name in this field.The MIME type of the BLOB data will be determined programatically.
  * **Fetch URL Field:**This is an option field. When user selects "BLOB/CLOB Field" radio button, then user can enter the name of column which holds the URL for fetching the content of primary BLOB/CLOB document. The value of this column will be used as display URL for corresponding  primary document.