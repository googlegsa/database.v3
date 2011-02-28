package com.google.enterprise.connector.db;


import java.sql.Connection;
import java.sql.SQLException;
import com.google.enterprise.connector.spi.RepositoryException;




public class DBConnectorConfigTest extends DBTestBase {


	@Override
	protected void setUp() throws Exception {
		super.setUp();
		runDBScript(CREATE_TEST_DB_TABLE);
		runDBScript(LOAD_TEST_DATA);
	}
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
			assertNotNull(connection);
		} catch (SQLException e) {
			System.out.println("SQLException");
			e.printStackTrace();
		} catch (RepositoryException e) {
			System.out.println("RepositoryException");
		}

	}


}
