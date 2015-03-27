Version 3.0 of the connector introduced the `MockClient`. This can be used
to stress test the connector in a live deployment without needing a
live database with large amounts of data.

There are two configuration changes to be made when creating a new connector instance:

  1. Configure the connector instance to use an in-memory H2 database, with a query that returns the required columns.
  1. Configure the `client` advanced configuration property in `connectorInstance.xml`.

### Configuring the connector instance ###

Here is a sample configuration form using an in-memory H2 database:

| Connector Hostname | 127.0.0.1 |
|:-------------------|:----------|
| JDBC Driver Classname | org.h2.Driver |
| JDBC Connection URL | jdbc:h2:mem: |
| Database Name | test |
| Username | sa |
| Password |  |
| SQL Query | SELECT 1 AS id, 'blob' as BIG |
| Primary Keys (separated by comma) | id |
| BLOB/CLOB Field | BIG |

The SQL query returns a result set that passes validation. In fact,
this is a working connector configuration all by itself, and it will
index a single row with the values '1' and 'blob'.

### Configuring the `client` property ###

> To configure the `MockClient`, add the following property to the
> `db-connector-config` bean in `connectorInstance.xml` (or under the
> Advanced Properties in the admin console):

```
    <property name="client">
       <bean class="com.google.enterprise.connector.db.testing.MockClient">
       </bean>
     </property>
```

> You can add properties to the `MockClient` bean to control the data
> returned by the connector. The following properties are supported:

#### rowCount ####
> The number of rows to return altogether.

> <em>Default Value:</em>
```
    <property name="rowCount" value="1000"/>
```

#### blobSize ####
> The size of the returned BLOBs.

> <em>Default Value:</em>
```
    <property name="blobSize" value="100000"/>
```

#### blobSingleton ####
> Use a single BLOB value for all rows (the default, to conserve
> memory and test other things), or create a new one for each row
> (to simulate real memory usage).

> <em>Default Value:</em>
```
    <property name="blobSingleton" value="true"/>
```

#### blobRandom ####
> Generate a BLOB value with random bytes or all zeroes (the default).

> <em>Default Value:</em>
```
    <property name="blobRandom" value="false"/>
```

#### Example ####

To configure the `MockClient` to return 50,000 rows with individually
randomized 500 KB BLOBs, use the following:

```
   <property name="client">
      <bean class="com.google.enterprise.connector.db.testing.MockClient">
         <property name="rowCount" value="50000"/>
         <property name="blobSize" value="500000"/>
         <property name="blobSingleton" value="false"/>
         <property name="blobRandom" value="true"/>
      </bean>
    </property>
```