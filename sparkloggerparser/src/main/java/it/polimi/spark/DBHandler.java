package it.polimi.spark;

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
			logger.error("Could not connect to DB, the application will not be added to the database");
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

	public void insertBenchmark(Benchmark application) throws SQLException {
		if (!succesfulConnection) {
			logger.warn("The initial connection to the database was not succesfull, application wil not be added");
			return;
		}

		insertIntoBenchmarkTable(application);

		for (Job job : application.getJobs())
			insertJob(job);

		for (RDD rdd : application.getRdds())
			insertRDD(rdd);

	}

	private void insertRDD(RDD rdd) throws SQLException {
		if (!succesfulConnection) {
			logger.warn("The initial connection to the database was not succesfull, RDD wil not be added");
			return;
		}
		insertIntoRDDTable(rdd);

	}

	private void insertJob(Job job) throws SQLException {
		if (!succesfulConnection) {
			logger.warn("The initial connection to the database was not succesfull, job wil not be added");
			return;
		}

		insertIntoJobTable(job);

		for (Stage stage : job.getStages())
			insertStage(stage);

	}

	private void insertStage(Stage stage) throws SQLException {
		if (!succesfulConnection) {
			logger.warn("The initial connection to the database was not succesfull, job wil not be added");
			return;
		}

		insertIntoStageTable(stage);

	}

	private void insertIntoBenchmarkTable(Benchmark application)
			throws SQLException {

		String insertQuery = " insert into Benchmark (";
		String values = " values (";

		insertQuery += "clusterName,";
		values += "'" + application.getClusterName() + "'" + ",";

		insertQuery += "appID,";
		values += "'" + application.getAppID() + "'" + ",";

		if (application.getAppName() != null) {
			insertQuery += "appName,";
			values += "'" + application.getAppName() + "'" + ",";
		}

		if (application.getDataSize() > 0) {
			insertQuery += "dataSize,";
			values += application.getDataSize() + ",";
		}

		if (application.getDuration() > 0) {
			insertQuery += "duration,";
			values += application.getDuration() + ",";
		}

		if (application.getParallelism() > 0) {
			insertQuery += "parallelism,";
			values += application.getParallelism() + ",";
		}

		if (application.getDriverMemory() > 0) {
			insertQuery += "driverMemory,";
			values += application.getDriverMemory() + ",";
		}

		if (application.getExecutorMemory() > 0) {
			insertQuery += "executorMemory,";
			values += application.getExecutorMemory() + ",";
		}

		if (application.getKryoMaxBuffer() > 0) {
			insertQuery += "kryoMaxBuffer,";
			values += application.getKryoMaxBuffer() + ",";
		}

		insertQuery += "rddCompress,";
		values += application.isRddCompress() + ",";

		if (application.getShuffleMemoryFraction() > 0) {
			insertQuery += "shuffleMemoryFraction,";
			values += application.getShuffleMemoryFraction() + ",";
		}

		if (application.getStorageMemoryFraction() > 0) {
			insertQuery += "storageMemoryFraction,";
			values += application.getStorageMemoryFraction() + ",";
		}

		if (application.getStorageLevel() != null) {
			insertQuery += "storageLevel,";
			values += "'" + application.getStorageLevel() + "'" + ",";
		}

		if (application.getExecutors() > 0) {
			insertQuery += "executors,";
			values += application.getExecutors() + ",";
		}

		if (application.getState() != null) {
			insertQuery += "state,";
			values += "'" + application.getState() + "'" + ",";
		}

		if (application.getLogFolder() != null) {
			insertQuery += "logFolder,";
			values += "'" + application.getLogFolder() + "'" + ",";
		}

		insertQuery = insertQuery.substring(0, insertQuery.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";

		String query = insertQuery + values;

		logger.trace("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		preparedStmt.execute();

	}

	private void insertIntoJobTable(Job job) throws SQLException {
		String insertQuery = " insert into Job (";
		String values = " values (";

		insertQuery += "clusterName,";
		values += "'" + job.getClusterName() + "'" + ",";

		insertQuery += "appID,";
		values += "'" + job.getAppID() + "'" + ",";

		insertQuery += "jobID,";
		values += job.getID() + ",";

		if (job.getDuration() > 0) {
			insertQuery += "duration,";
			values += job.getDuration() + ",";
		}

		insertQuery = insertQuery.substring(0, insertQuery.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";

		String query = insertQuery + values;

		logger.trace("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		preparedStmt.execute();

	}

	private void insertIntoStageTable(Stage stage) throws SQLException {
		String insertQuery = " insert into Stage (";
		String values = " values (";

		insertQuery += "clusterName,";
		values += "'" + stage.getClusterName() + "'" + ",";

		insertQuery += "appID,";
		values += "'" + stage.getAppID() + "'" + ",";

		insertQuery += "jobID,";
		values += stage.getJobID() + ",";

		insertQuery += "stageID,";
		values += stage.getID() + ",";

		if (stage.getDuration() > 0) {
			insertQuery += "duration,";
			values += stage.getDuration() + ",";
		}

		if (stage.getInputSize() > 0) {
			insertQuery += "inputSize,";
			values += stage.getInputSize() + ",";
		}

		if (stage.getOutputSize() > 0) {
			insertQuery += "outputSize,";
			values += stage.getOutputSize() + ",";
		}

		if (stage.getShuffleReadSize() > 0) {
			insertQuery += "shuffleReadSize,";
			values += stage.getShuffleReadSize() + ",";
		}

		if (stage.getShuffleWriteSize() > 0) {
			insertQuery += "shuffleWriteSize,";
			values += stage.getShuffleWriteSize() + ",";
		}

		insertQuery = insertQuery.substring(0, insertQuery.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";

		String query = insertQuery + values;

		logger.trace("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		preparedStmt.execute();

	}

	private void insertIntoRDDTable(RDD rdd) throws SQLException {
		String insertQuery = " insert into Rdd (";
		String values = " values (";

		insertQuery += "rddID,";
		values += "'" + rdd.getID() + "'" + ",";

		insertQuery += "appID,";
		values += "'" + rdd.getAppID() + "'" + ",";

		insertQuery += "clusterName,";
		values += "'" + rdd.getClusterName() + "'" + ",";

		if (rdd.getName() != null) {
			insertQuery += "name,";
			values += rdd.getName() + ",";
		}

		if (rdd.getStageID() >= 0) {
			insertQuery += "stageID,";
			values += rdd.getStageID() + ",";
		}

		if (rdd.getScope() != null) {
			insertQuery += "scope,";
			values += rdd.getScope().replaceAll(",", ";") + ",";
		}

		insertQuery += "useDisk,";
		values += rdd.isUseDisk() + ",";
		
		insertQuery += "useMemory,";
		values += rdd.isUseMemory() + ",";
		
		insertQuery += "deserialized,";
		values += rdd.isDeserialized() + ",";

		if (rdd.getNumberOfPartitions() >= 0) {
			insertQuery += "numberOfPartitions,";
			values += rdd.getNumberOfPartitions() + ",";
		}
		
		if (rdd.getNumberOfCachedPartitions() >= 0) {
			insertQuery += "cachedPartitions,";
			values += rdd.getNumberOfCachedPartitions() + ",";
		}

		if (rdd.getMemorySize() >= 0) {
			insertQuery += "memorySize,";
			values += rdd.getMemorySize() + ",";
		}
		
		if (rdd.getDiskSize() >= 0) {
			insertQuery += "diskSize,";
			values += rdd.getDiskSize()+ ",";
		}

		insertQuery = insertQuery.substring(0, insertQuery.length() - 1) + ")";
		values = values.substring(0, values.length() - 1) + ")";

		String query = insertQuery + values;

		logger.info("Sending Query: " + query);
		// create the mysql insert prepared statement
		PreparedStatement preparedStmt = conn.prepareStatement(query);
		// execute the prepared statement
		preparedStmt.execute();

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
