package it.polimi.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerParser {

	static final Logger logger = LoggerFactory.getLogger(LoggerParser.class);
	static Config config;
	static SQLContext sqlContext;
	static FileSystem hdfs;

	public static void main(String[] args) throws IOException,
			URISyntaxException {

		// the configuration of the application (as launched by the user)
		Config.init(args);
		config = Config.getInstance();
		if (!Files.exists(Paths.get(config.inputFile))) {
			logger.error("Input file does not exist");
			return;
		}

		// the spark configuration
		SparkConf conf = new SparkConf().setAppName("logger-parser").setMaster(
				"local[1]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// sqlContext = new org.apache.spark.sql.SQLContext(sc); To use SparkSQL
		// dialect instead of Hive	
		sqlContext = new HiveContext(sc.sc());

		// the hadoop configuration
		Configuration hadoopConf = new Configuration();
		hdfs = FileSystem.get(hadoopConf);
		if (hdfs.exists(new Path(config.outputFile)))
			hdfs.delete(new Path(config.outputFile), true);

		// load the logs
		DataFrame logsframe = sqlContext.jsonFile(config.inputFile);
		logsframe.cache();

		// register the main table with all the logs as "events"
		logsframe.registerTempTable("events");

		retrieveTaskInformation();

		retrieveStageInformation();

		hdfs.close();
	}

	private static void retrieveStageInformation() {
		// register two tables, one for the Stgae start event and the other for
		// the Stage end event
		sqlContext.sql(
				"SELECT * FROM events WHERE Event LIKE '%StageSubmitted'")
				.registerTempTable("stageStartInfos");
		sqlContext.sql(
				"SELECT * FROM events WHERE Event LIKE '%StageCompleted'")
				.registerTempTable("stageEndInfos");

		// register a table with RDD information
		sqlContext
				.sql("SELECT `stageEndInfos.Stage Info.RDD Info` FROM stageEndInfos")
				.registerTempTable("RDDInfos");

		sqlContext
				.sql("SELECT `stageEndInfos.Stage Info.RDD Info.RDD ID` FROM stageEndInfos")
				.show();
		sqlContext
				.sql("SELECT `stageEndInfos.Stage Info.RDD Info` FROM stageEndInfos")
				.printSchema();
		// sqlContext.sql("SELECT `RDD Info.RDD ID` from RDDInfos").show();;

		// query the two tables for the task details
		DataFrame stageDetails = sqlContext
				.sql("SELECT 	`start.Stage Info.Stage ID` AS id,"
						+ "		`start.Stage Info.Stage Name` AS name,"
						+ "		`start.Stage Info.Number of Tasks` AS numberOfTasks,"
						+ "		`start.Stage Info.Details` AS details,"
						+ "		`finish.Stage Info.Submission Time` AS submissionTime,"
						+ "		`finish.Stage Info.Completion Time` AS completionTime,"
						+ "		`finish.Stage Info.Completion Time` - `finish.Stage Info.Submission Time` AS executionTime,"
						+ "		`start.Stage Info.RDD Info` AS rddInfo"
						+ "		FROM stageStartInfos AS start"
						+ "		JOIN stageEndInfos AS finish"
						+ "		ON `start.Stage Info.Stage ID`=`finish.Stage Info.Stage ID`");

		stageDetails.show();

	}

	/**
	 * Retrieves the information on the Tasks
	 * 
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	private static void retrieveTaskInformation() throws IOException,
			UnsupportedEncodingException {
		// register two tables, one for the task start event and the other for
		// the task end event
		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%TaskStart'")
				.registerTempTable("taskStartInfos");
		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%TaskEnd'")
				.registerTempTable("taskEndInfos");

		// query the two tables for the task details
		DataFrame taskDetails = sqlContext
				.sql("SELECT 	`start.Task Info.Task ID` AS id,"
						+ "				`start.Stage ID` AS stageID,"
						+ "				`start.Task Info.Executor ID` AS executorID,"
						+ "				`start.Task Info.Host` AS host,"
						+ "				`finish.Task Type` AS type,"
						+ "				`finish.Task Info.Finish Time` - `start.Task Info.Launch Time` AS executionTime,"
						+ "				`finish.Task Info.Finish Time`  AS finishTime,"
						+ "				`start.Task Info.Launch Time` AS startTime,"
						+ "				`finish.Task Metrics.Executor Run Time` AS executorRunTime,"
						+ "				`finish.Task Metrics.Executor Deserialize Time` AS executorDeserializerTime,"
						+ "				`finish.Task Metrics.Result Serialization Time` AS resultSerializationTime,"
						+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Write Time` AS shuffleWriteTime,"
						+ "				`finish.Task Metrics.JVM GC Time` AS GCTime,"
						+ "				`finish.Task Metrics.Result Size` AS resultSize,"
						+ "				`finish.Task Metrics.Memory Bytes Spilled` AS memoryBytesSpilled,"
						+ "				`finish.Task Metrics.Disk Bytes Spilled` AS diskBytesSpilled,"
						+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written` AS shuffleBytesWritten,"
						+ "				`finish.Task Metrics.Shuffle Write Metrics.Shuffle Records Written` AS shuffleRecordsWritten,"
						+ "				`finish.Task Metrics.Input Metrics.Data Read Method` AS dataReadMethod,"
						+ "				`finish.Task Metrics.Input Metrics.Bytes Read` AS bytesRead,"
						+ "				`finish.Task Metrics.Input Metrics.Records Read` AS recordsRead,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Remote Blocks Fetched` AS shuffleRemoteBlocksFetched,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Local Blocks Fetched` AS shuffleLocalBlocksFetched,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Fetch Wait Time` AS shuffleFetchWaitTime,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Remote Bytes Read` AS shuffleRemoteBytesRead,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Local Bytes Read` AS shuffleLocalBytesRead,"
						+ "				`finish.Task Metrics.Shuffle Read Metrics.Total Records Read` AS shuffleTotalRecordsRead"
						+ "		FROM taskStartInfos AS start"
						+ "		JOIN taskEndInfos AS finish"
						+ "		ON `start.Task Info.Task ID`=`finish.Task Info.Task ID`");
		// register the result as a table
		taskDetails.registerTempTable("tasks");
		saveListToCSV(taskDetails, "TaskDetails.csv");
	}

	/**
	 * Saves the table in the specified dataFrame in a CSV file. In order to
	 * save the whole table into a single the DataFrame is transformed into and
	 * RDD and then elements are collected. This might cause performance issue
	 * if the table is too long
	 * 
	 * @param data
	 * @param fileName
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	private static void saveListToCSV(DataFrame data, String fileName)
			throws IOException, UnsupportedEncodingException {
		List<Row> taskList = data.toJavaRDD().collect();
		OutputStream os = hdfs.create(new Path(config.outputFile, fileName));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		// the schema first
		for (String column : data.columns())
			br.write(column + ",");
		br.write("\n");
		// the values later
		for (Row task : taskList) {
			for (int i = 0; i < task.size(); i++)
				br.write(task.get(i) + ",");
			br.write("\n");
		}
		br.close();
	}

}
