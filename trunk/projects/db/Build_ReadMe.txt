Following are the steps to build Google Search Appliance Connector for Databases
================================================================================

1. Ensure that you have Apache Ant 1.8, Subversion 1.6, Tomcat 7 and JDK 1.6
   installed on your system.  For details, see the "Prerequisites" section here:
   http://code.google.com/p/google-enterprise-connector-manager/wiki/DeveloperEnvironmentSetup

2. Ensure that you have Connector Manager on your system. You can get it at
   http://code.google.com/p/google-enterprise-connector-manager/source/checkout
   It is recommended you check out a stable branch of the Connector Manager
   instead of the trunk.
   2a) svn checkout https://google-enterprise-connector-manager.googlecode.com/svn/branches/3.0.x/ google-enterprise-connector-manager
   2b) Follow the instructions for building the Connector Manager as described here:
       http://code.google.com/p/google-enterprise-connector-manager/wiki/DeveloperEnvironmentSetup
   2c) Execute ANT target "ant install-all" to build the Connector Manager jar
       files.  If you get compile errors "package javax.jcr does not exist",
       go back to the previous step and make sure JCR was installed correctly.

3. Create a file 'google-enterprise-connector-db.properties' in your home directory
   (or wherever Java's user.home System property thinks your home directory is).
   Set the property 'build.connector.manager.home', pointing to the root of the
   Connector Manager checkout directory (created in step 2a).  For example:
     build.connector.manager.home=C:/google-enterprise-connector-manager/

4. From the command prompt, change directory to database connector home
   directory (where build.xml file is present), execute the ANT target
   "ant jar_prod" to build the Database Connector jar file.  Jar file
   "connector-db.jar" will be built under "./build/prod/jar/" directory.

5. Steps to run JUnit test cases:
   Execute ANT target "ant run_tests" to run JUnit test cases.
   All the JUnit test cases should run successfully using the build-in
   H2 database.

   If you wish to test against an external third party database, such as
   Oracle or SQL-Server, you will need to do the following:

   5a) Copy the required database driver jar file to "third_party\tests\"
       directory of database connector home. User needs to download appropriate
       database driver jar file from the web. For details of different JDBC
       drivers for various databases you can refer to
       http://www.dbvis.com/products/dbvis/doc/supports.jsp

   5b) Modify "third_party\tests\DatabaseConfiguration.properties" file as per
       your test database settings. Refer below example for configuring test
       database.
         # Provide the Login name of database user.
         login=admin
         # Provide the Password of database user
         password=password
         # This is for MySQL database
         driverClassName=com.mysql.jdbc.Driver
         # Provide the connection URL for your database
         connectionUrl=protocol://db-server:port/DB_Name
         # Provide name of test database
         dbName=MyDB
         # Provide fully qualified connector host name
         hostname=machine.domain
       Note: Do not modify the values of the sqlQuery or primaryKeysString
       properties.

   5c) Execute ANT target "ant run_tests" to run JUnit test cases as described
       above.

6. Execute ANT target "ant download_db_connector" to create deliverables
   (source-code and binary) under "<database connector home directory>/downloads".
