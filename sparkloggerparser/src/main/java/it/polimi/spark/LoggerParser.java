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
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.StringNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.ArrayBuffer;

public class LoggerParser {

	static Config config;
	static FileSystem hdfs;
	static final Logger logger = LoggerFactory.getLogger(LoggerParser.class);
	static SQLContext sqlContext;
	static final String STAGE_LABEL = "Stage_";
	static final String RDD_LABEL = "RDD_";

	public static void main(String[] args) throws IOException,
			URISyntaxException, ClassNotFoundException {

		// the configuration of the application (as launched by the user)
		Config.init(args);
		config = Config.getInstance();
		if (!Files.exists(Paths.get(config.inputFile))) {
			logger.error("Input file does not exist");
			return;
		}

		// the spark configuration
		SparkConf conf = new SparkConf().setAppName("logger-parser");
		if (config.runLocal)
			conf.setMaster("local[1]");
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

		// query the data
		DataFrame taskDetails = retrieveTaskInformation();

		DataFrame stageDetails = retrieveStageInformation();

		// save CSV with performance information
		saveListToCSV(taskDetails, "TaskDetails.csv");

		saveListToCSV(stageDetails, "StageDetails.csv");

		// save the graph dot files for visualization
		printStageGraph(stageDetails);
		printRDDGraph(stageDetails);

		// clean up the mess
		hdfs.close();
		sc.close();
	}

	/**
	 * builds the graph with RDD dependencies and saves it into a .dot file that
	 * can be used for visualization
	 * 
	 * @param stageDetails
	 * @throws IOException
	 */
	private static void printRDDGraph(DataFrame stageDetails)
			throws IOException {
		DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(
				DefaultEdge.class);
		// add all vertexes first
		for (Row row : stageDetails.select("RDD ID").distinct().collectAsList()) {
			dag.addVertex(RDD_LABEL + row.getLong(0));
			logger.info("Added " + RDD_LABEL + row.getLong(0) + " to the graph");
		}

		// add all edges
		for (Row row : stageDetails.select("RDD ID", "RDDParentIDs")
				.collectAsList()) {
			ArrayBuffer<?> sources = (ArrayBuffer<?>) row.get(1);
			for (String source : sources.mkString(",").split(","))
				if (source != null && !source.isEmpty()) {
					dag.addEdge(RDD_LABEL + source, RDD_LABEL + row.getLong(0));
					logger.info("Added link from " + RDD_LABEL + source + "to "
							+ RDD_LABEL + row.getLong(0));
				}
		}

		DOTExporter<String, DefaultEdge> exporter = new DOTExporter<String, DefaultEdge>(
				new StringNameProvider<String>(), null, null);
		OutputStream os = hdfs.create(new Path(config.outputFile,
				"rdd-graph.dot"));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();

	}

	/**
	 * builds the graph with stages dependencies and saves it into a .dot file
	 * that can be used for visualization
	 * 
	 * @param stageDetails
	 * @throws IOException
	 */
	private static void printStageGraph(DataFrame stageDetails)
			throws IOException {

		DirectedAcyclicGraph<String, DefaultEdge> dag = new DirectedAcyclicGraph<String, DefaultEdge>(
				DefaultEdge.class);
		// add all vertexes first
		for (Row row : stageDetails.select("id").distinct().collectAsList()) {
			dag.addVertex(STAGE_LABEL + row.getLong(0));
			logger.info("Added " + STAGE_LABEL + row.getLong(0)
					+ " to the graph");
		}

		// add all edges
		for (Row row : stageDetails.select("id", "parentIDs").distinct()
				.collectAsList()) {
			ArrayBuffer<?> sources = (ArrayBuffer<?>) row.get(1);
			for (String source : sources.mkString(",").split(","))
				if (source != null && !source.isEmpty()) {
					dag.addEdge(STAGE_LABEL + source,
							STAGE_LABEL + row.getLong(0));
					logger.info("Added link from " + STAGE_LABEL + source
							+ "to " + STAGE_LABEL + row.getLong(0));
				}
		}

		DOTExporter<String, DefaultEdge> exporter = new DOTExporter<String, DefaultEdge>(
				new StringNameProvider<String>(), null, null);
		OutputStream os = hdfs.create(new Path(config.outputFile,
				"stage-graph.dot"));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();

	}

	/**
	 * Retrieves the information for stages
	 * 
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	private static DataFrame retrieveStageInformation()
			throws UnsupportedEncodingException, IOException {
		// register two tables, one for the Stgae start event and the other for
		// the Stage end event
		sqlContext.sql(
				"SELECT * FROM events WHERE Event LIKE '%StageSubmitted'")
				.registerTempTable("stageStartInfos");
		sqlContext.sql(
				"SELECT * FROM events WHERE Event LIKE '%StageCompleted'")
				.registerTempTable("stageEndInfos");

		// expand the nested structure of the RDD Info and register as a
		// temporary table
		sqlContext
				.sql("	SELECT `Stage Info.Stage ID`, RDDInfo"
						+ "		FROM stageEndInfos LATERAL VIEW explode(`Stage Info.RDD Info`) rddInfoTable AS RDDInfo")
				.registerTempTable("rddInfos");

		// merge the three tables to get the desired information
		DataFrame stageDetails = sqlContext
				.sql("SELECT 	`start.Stage Info.Stage ID` AS id,"
						+ "		`start.Stage Info.Parent IDs` AS parentIDs,"
						+ "		`start.Stage Info.Stage Name` AS name,"
						+ "		`start.Stage Info.Number of Tasks` AS numberOfTasks,"
						+ "		`finish.Stage Info.Submission Time` AS submissionTime,"
						+ "		`finish.Stage Info.Completion Time` AS completionTime,"
						+ "		`finish.Stage Info.Completion Time` - `finish.Stage Info.Submission Time` AS executionTime,"
						+ "		`rddInfo.RDD ID`,"
						// + "		`rddInfo.Scope` AS RDDScope," //TODO: disabled
						// until we find a way to correctly parse this
						+ "		`rddInfo.Name` AS RDDName,"
						+ "		`rddInfo.Parent IDs` AS RDDParentIDs,"
						+ "		`rddInfo.Storage Level.Use Disk`,"
						+ "		`rddInfo.Storage Level.Use Memory`,"
						+ "		`rddInfo.Storage Level.Use ExternalBlockStore`,"
						+ "		`rddInfo.Storage Level.Deserialized`,"
						+ "		`rddInfo.Storage Level.Replication`,"
						+ "		`rddInfo.Number of Partitions`,"
						+ "		`rddInfo.Number of Cached Partitions`,"
						+ "		`rddInfo.Memory Size`,"
						+ "		`rddInfo.ExternalBlockStore Size`,"
						+ "		`rddInfo.Disk Size`"
						+ "		FROM stageStartInfos AS start"
						+ "		JOIN stageEndInfos AS finish"
						+ "		ON `start.Stage Info.Stage ID`=`finish.Stage Info.Stage ID`"
						+ "		JOIN rddInfos"
						+ "		ON `start.Stage Info.Stage ID`=`rddInfos.Stage ID`");

		stageDetails.registerTempTable("stages");

		return stageDetails;

	}

	/**
	 * Retrieves the information on the Tasks
	 * 
	 * @return
	 * 
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	private static DataFrame retrieveTaskInformation() throws IOException,
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
						+ "				`finish.Task Info.Getting Result Time`  AS gettingResultTime,"
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
		return taskDetails;
	}

	/**
	 * Saves the table in the specified dataFrame in a CSV file. In order to
	 * save the whole table into a single the DataFrame is transformed into and
	 * RDD and then elements are collected. This might cause performance issue
	 * if the table is too long If a field contains an array (ArrayBuffer) its
	 * content is serialized with spaces as delimiters
	 * 
	 * @param data
	 * @param fileName
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	private static void saveListToCSV(DataFrame data, String fileName)
			throws IOException, UnsupportedEncodingException {
		List<Row> table = data.toJavaRDD().collect();
		OutputStream os = hdfs.create(new Path(config.outputFile, fileName));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		// the schema first
		for (String column : data.columns())
			br.write(column + ",");
		br.write("\n");
		// the values after
		for (Row row : table) {
			for (int i = 0; i < row.size(); i++) {
				if (row.get(i) instanceof ArrayBuffer<?>)
					br.write(((ArrayBuffer<?>) row.get(i)).mkString(" ") + ',');
				else
					br.write(row.get(i) + ",");
			}
			br.write("\n");
		}
		br.close();
	}

}
