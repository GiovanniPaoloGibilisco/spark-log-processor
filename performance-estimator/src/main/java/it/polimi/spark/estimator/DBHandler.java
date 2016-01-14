package it.polimi.spark.estimator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBHandler {

	public static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";
	String driver = "com.mysql.jdbc.Driver";
	String url;
	String user;
	String password;
	Connection conn;
	boolean succesfulConnection;
	static final Logger logger = LoggerFactory.getLogger(DBHandler.class);

	public DBHandler(String driver, String url, String user, String password) {
		this.driver = driver;
		this.url = url;
		this.user = user;
		this.password = password;
		try {
			initConnection();
			succesfulConnection = true;
		} catch (Exception e) {
			logger.error("Could not connect to DB, the application will not be added to the database",e);
			succesfulConnection = false;
		}
	}

	public DBHandler(String url, String user, String password) {
		this(DEFAULT_DRIVER, url, user, password);
	}

	private void initConnection() throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
	}

	public void updateApplicationExpectedExecutionTime(String clusterName,
			String appID, double expectedTime) throws SQLException {

		String query = "UPDATE benchmark SET estimatedDuration='"
				+ expectedTime + "' WHERE appID='" + appID
				+ "' AND clusterName='" + clusterName + "'";
		logger.info("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		int count = preparedStmt.executeUpdate();
		if(count == 0)
			logger.warn("Time for Application "+appID+" was not updated.");

	}

	public void updateJobExpectedExecutionTime(String clusterName,
			String appID, int jobId, double expectedTime) throws SQLException {

		String query = "UPDATE job SET estimatedDuration='" + expectedTime
				+ "' WHERE appID='" + appID + "' AND clusterName='"
				+ clusterName + "'" + " AND jobID='" + jobId + "'";
		logger.info("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		int count = preparedStmt.executeUpdate();
		if(count == 0)
			logger.warn("Time for Job "+jobId+" was not updated.");

	}

	public void updateStageExpectedExecutionTime(String clusterName,
			String appID, int jobId, int stageId, double expectedTime)
			throws SQLException {

		String query = "UPDATE stage SET estimatedDuration='" + expectedTime
				+ "' WHERE appID='" + appID + "' AND clusterName='"
				+ clusterName + "'" + " AND jobID='" + jobId + "'"
				+ " AND stageID='" + stageId + "'";
		logger.info("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		int count = preparedStmt.executeUpdate();
		if(count == 0)
			logger.warn("Time for Stage "+stageId+" was not updated.");

	}

	public void close() {
		if (succesfulConnection)
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Unable to close connection with te db");
			}
	}

}
