# Introduction #

This document describes steps to be followed when upgrading from database connector version 2.8.0 to the 2.8.1-X1 performance patch. For instructions on using the 2.8.4 release, see IndexingPerformance.


# Pre-requisites #
  * Apache Tomcat 6. You can download it from it from http://tomcat.apache.org/download-60.cgi
  * JRE1.5 or above. Refer to http://java.sun.com/javase/downloads/index_jdk5.jsp for downloads
  * Connector Manager version 2.8.0 or higher.Refer to  http://code.google.com/p/google-enterprise-connector-manager/downloads/list for downloads.
  * Database Connector version 2.8.0 is deployed and a connector instance for Database Connector is configured.
  * A total ordering on the database table is Required for using the ordering to select the next batch of documents using minvalue and maxvalue range

# Upgrade Steps #
  1. Shut down Tomcat if it is running.
  1. Go to $CATALINA\_HOME/webapps/connector-manager/WEB-INF/lib and replace the old connector-db jar with new connector-db jar.
  1. Go to $CATALINA\_HOME/webapps/connector-manager/WEB-INF/connectors/DBConnector/$CONNECTOR\_NAME directory.
    * **edit Add the following lines in connectorInstance.xml file:
```
      Add the following lines at the end of the file just before "</bean></beans>":
     <!--
     Range to be crawled when a parameterized crawl query is entered. 
     Replace the minValue and MaxValue with actual range values respectively
      -->
    
    <property name="minValue" value="0"></property>
    <property name="maxValue" value="0"></property>
    

    <!--
    Flag to determine whether the SQl Crawl is parameterized
    -->
    <property name="parameterizedQueryFlag" value="true"></property>
```
> > Note:Make sure that the values for properties "minvalue" and "maxvalue" have their values set equal to the range in previous crawl Query.
```
   For Example:If the SQL crawl query was:

sqlQuery=select emp_name from employee where emp_id between 1 and 100

then set 
    <property name="minValue" value="1"></property>
    <property name="maxValue" value="100"></property>
```
> > If  values for properties "minvalue" and "maxvalue"  are different than previous crawl query range then connector would send delete feeds for previously crawled records first and then start traversing the new range.**

  * edit $CONNECTOR\_NAME.properties file to modify the sqlQuery and replace the range to be crawled with #minvalue# and #maxvalue#.
```
       For Example:Modify the below SQL query

     sqlQuery=select * from employee where emp_id between 1 and 100
                            TO
     sqlQuery=select * from table where emp_id between #minvalue#  and #maxvalue#
```
    * edit IbatisSqlMap.xml file to modify the replace it with the new sqlQuery value:
```
	     For Example:Modify the element as below
        <select id="getAll" resultClass="java.util.HashMap"> 
           select * from employee where emp_id between 1 and 100
         </select> 
                            TO
        <select id="getAll" resultClass="java.util.HashMap"> 
           select * from employee where emp_id between #minvalue# and #maxvalue#
        </select> 
```


> 3. Restart Tomcat