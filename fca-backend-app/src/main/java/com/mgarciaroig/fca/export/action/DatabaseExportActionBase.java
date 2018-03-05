package com.mgarciaroig.fca.export.action;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mgarciaroig.pfc.fca.framework.oozie.BaseAction;

/**
 * Base class with utility methods to implement database export actions
 * @author Miguel Ángel García Roig (mgarciaroig@uoc.edu)
 *
 */
abstract class DatabaseExportActionBase extends BaseAction {
	
	private static final String databaseDriverProperty = "jdbc.driver.name";
	private static final String databaseConnectionStringProperty = "jdbc.connection.string";
	private static final String userProperty = "user";
	private static final String passwordProperty = "password";

	protected DatabaseExportActionBase(String[] args) throws IOException {
		super(args);		
	}
	
	protected Connection connectToDatabase() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
						
		Class.forName(super.getPropertyFromConfiguration(databaseDriverProperty)).newInstance();
		
		String connectionString = super.getPropertyFromConfiguration(databaseConnectionStringProperty);
		String user = super.getPropertyFromConfiguration(userProperty);
		String password = super.getPropertyFromConfiguration(passwordProperty);
				
		final Connection con = DriverManager.getConnection(connectionString, user, password);
		
		con.setAutoCommit(false);			
		
		return con;
	}	
	
	protected void cleanUpDatabase(final Connection con, final String... toCleanUpTables) throws SQLException{
						
		for (final String currentTableToCleanUp : toCleanUpTables){
			
			cleanUpTable(con, currentTableToCleanUp);		
		}		
	}
	
	protected int lastInsertId(final PreparedStatement st) throws SQLException {
		
		final ResultSet keys = st.getGeneratedKeys();
		keys.next();
		
		return keys.getInt(1);		
	}
	
	private void cleanUpTable(final Connection con, final String tableName) throws SQLException {
		
		try (final Statement st = con.createStatement()){
			st.execute(buildCleanUpSql(tableName));
		}
	}
	
	private String buildCleanUpSql(final String tableName) {
		return "truncate ".concat(tableName);
	}
	

}
