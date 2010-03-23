Following are the steps to build Google Search Appliance Connector for Databases
========================================================================================
1. Ensure that you have Apache Ant installed on your system. If not, you can get it from http://ant.apache.org/bindownload.cgi

2. Ensure that you have Connector Manager on your system. You can get it at
   http://code.google.com/p/google-enterprise-connector-manager/downloads/list.
   Choose connector-manager-2.4.4.zip  (binary).
   2b) Unpack the zip file. (unzip connector-manager-2.4.4.zip).
   2c) Change directory to connector-manager-2.4.4 (cd connector-manager-2.4.4).
   2d) Rename connector-manager.war to connector-manager.zip
   2e) Unpack the connector-manager.zip file(unzip connector-manager.zip).
   2f) Create Connector Manager Home directory in file system.
   2g) Create directory structure "dist\jarfile" under Connector Manager home directory.
   2h) Copy jar files from "connector-manager\WEB-INF\lib"(created in step 2e) to "Connector Manager home directory\dist\jarfile" directory.

3. Create a google-enterprise-connector-db.properties file in your home directory(where database connector project is downloaded ). Add a line to set the
   connector-manager-projects.dir pointing to the Connector Manager Home directory(created in step 2f). You can use the line below as an example but must replace
   ##.Connector Manager Home.## with the actual path to the connector manager home directory on your system.
   connector-manager-projects.dir=##.Connector Manager Home.##

4. From the command prompt, change directory to database connector home directory(where build.xml file is present),
   execute the ANT target "ant jar_prod" to build the Database Connector jar file. Jar file "connector-db.jar" will be built under "./build/prod/jar/" directory.

5. Steps to run JUnit test cases
    5a) Modify "config\DatabaseConfiguration.properties" file as per your test database settings. Refer below example for configuring test database.
            #Provide the Login name of database user.
            login=admin
            #Provide the Password of database user
            password=password
            #This is for MySQL database
            driverClassName=com.mysql.jdbc.Driver
            ##Provide the connection URL for your database
            connectionUrl=protocol://db-server:port/DB_Name
            #Provide name of test database
            dbName=MyDB
            #Provide fully qualified connector host name
            hostname=machine.domain

    5b) Copy database driver jar file to "third_party\prod\" directory of database connector home. User needs to download appropriate
        database driver jar file from the web. For details of different JDBC drivers for various databases you can refer to http://www.dbvis.com/products/dbvis/doc/supports.jsp
    5c) Copy JUnit jar to "third_party\tests\" directory of database connector home. User needs to download JUnit3 jar from     http://sourceforge.net/projects/junit/files/junit/.
    5d) Execute ant target "ant run_tests" to run JUnit test cases. All the JUnit test cases should run successfully.

6. Execute ANT target "ant download_db_connector" to create deliverables(source-code and binary) under "<database connector home directory>/downloads".