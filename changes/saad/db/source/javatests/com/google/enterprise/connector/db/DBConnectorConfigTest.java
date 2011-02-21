package com.google.enterprise.connector.db;


import java.sql.Connection;
import java.sql.SQLException;

import com.google.enterprise.connector.spi.RepositoryException;




public class DBConnectorConfigTest extends DBTestBase {

	public void testDBClient(){

		try {
			DBConnectorConfig dbConnectorConfig=getDBConnectorConfig();
			assertNotNull(dbConnectorConfig.getDbClient());
		} catch (RepositoryException e) {
			System.out.println("RepositoryException");
		}

	}

	public void testDBContext(){

		try {
			DBConnectorConfig dbConnectorConfig=getDBConnectorConfig();

			assertNotNull(dbConnectorConfig.getDbContext());

		} catch (RepositoryException e) {
			System.out.println("RepositoryException");
		}



	}


	public void testConnectivity() {

		Connection connection;
		try {
			connection = getDBConnectorConfig().getDbClient().getSqlMapClient().getDataSource().getConnection();
			assertEquals(true,connection);
		} catch (SQLException e) {
			System.out.println("SQLException");
			e.printStackTrace();
		} catch (RepositoryException e) {
			System.out.println("RepositoryException");
		}

	}


}
