package it.polimi.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

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
import org.jgrapht.ext.VertexNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class LoggerParser {

	static Config config;
	static FileSystem hdfs;
	static final Logger logger = LoggerFactory.getLogger(LoggerParser.class);
	static SQLContext sqlContext;
	static final String STAGE_LABEL = "Stage_";
	static final String JOB_LABEL = "Job_";
	static final String RDD_LABEL = "RDD_";

	public static void main(String[] args) throws IOException,
			URISyntaxException, ClassNotFoundException {

		// the hadoop configuration
		Configuration hadoopConf = new Configuration();
		hdfs = FileSystem.get(hadoopConf);

		// the spark configuration
		SparkConf conf = new SparkConf().setAppName("logger-parser");
		// the configuration of the application (as launched by the user)
		Config.init(args);
		config = Config.getInstance();

		if (config.runLocal)
			conf.setMaster("local[1]");

		// either -i or -a has to be specified
		if (config.inputFile == null && config.applicationID == null) {
			logger.info("No input file (-i option) or application id (-a option) has been specified. At least one of these options has to be provided");
			return;
		}

		// exactly one otherwise we will not know where the user wants to get
		// the logs from
		if (config.inputFile != null && config.applicationID != null) {
			logger.info("Either the input file (-i option) or the application id (-a option) has to be specified.");
			return;
		}

		// if the -a option has been specified, then get the default
		// logging directory from sparkconf (property spark.eventLog.dir) and
		// use it as base folder to look for the application log
		if (config.applicationID != null) {
			String eventLogDir = conf.get("spark.eventLog.dir", null).replace(
					"file://", "");
			if (eventLogDir == null) {
				logger.info("Could not retireve the logging directory from the spark configuration, the property spark.eventLog.dir has to be set");
				return;
			}
			config.inputFile = eventLogDir + "/" + config.applicationID;
		}

		logger.info("Reding logs from: " + config.inputFile);

		// if the file does not exist
		if (config.inputFile != null
				&& !hdfs.exists(new Path(config.inputFile))) {
			logger.info("Input file " + config.inputFile + " does not exist");
			return;
		}

		JavaSparkContext sc = new JavaSparkContext(conf);
		// sqlContext = new org.apache.spark.sql.SQLContext(sc); To use SparkSQL
		// dialect instead of Hive
		sqlContext = new HiveContext(sc.sc());

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

		List<RDD> rdds = extractRDDs(stageDetails);
		List<Stage> stages = extractStages(stageDetails);

		// save the graph dot files for visualization
		printStageGraph(stages);
		printRDDGraph(rdds);

		// clean up the mess
		hdfs.close();
		sc.close();
	}

	private static List<Stage> extractStages(DataFrame stageDetails) {
		List<Stage> stages = new ArrayList<Stage>();
		for (Row row : stageDetails.select("Job ID", "id", "parentIDs", "name").distinct()
				.collectAsList()) {
			List<Long> parentList = null;
			if (row.get(2) instanceof scala.collection.immutable.List<?>)
				parentList = JavaConversions.asJavaList((Seq<Long>) row.get(2));
			else if (row.get(2) instanceof ArrayBuffer<?>)
				parentList = JavaConversions.asJavaList((ArrayBuffer<Long>) row
						.get(2));
			else {
				logger.warn("Could not parse Stage Parent IDs Serialization:"
						+ row.get(2).toString() + " class: "
						+ row.get(2).getClass() + " Object: " + row.get(2));
			}
			stages.add(new Stage(row.getLong(0), row.getLong(1), parentList,
					row.getString(3)));

		}

		return stages;
	}

	/**
	 * Extracts a list of RDDs from the table
	 * 
	 * @param stageDetails
	 * @return list of RDDs
	 */
	private static List<RDD> extractRDDs(DataFrame stageDetails) {
		List<RDD> rdds = new ArrayList<RDD>();
		for (Row row : stageDetails.select("RDD ID", "RDDParentIDs", "RDDName",
				"RDDScope", "Number of Partitions").collectAsList()) {
			List<Long> parentList = null;
			if (row.get(1) instanceof scala.collection.immutable.List<?>)
				parentList = JavaConversions.asJavaList((Seq<Long>) row.get(1));
			else if (row.get(1) instanceof ArrayBuffer<?>)
				parentList = JavaConversions.asJavaList((ArrayBuffer<Long>) row
						.get(1));
			else {
				logger.warn("Could not parse RDD PArent IDs Serialization:"
						+ row.get(1).toString() + " class: "
						+ row.get(1).getClass() + " Object: " + row.get(1));
			}

			long scopeID = 0;
			String scopeName = null;
			if (row.get(3) != null && !row.getString(3).isEmpty()
					&& row.getString(3).startsWith("{")) {
				JsonObject scopeObject = new JsonParser().parse(
						row.getString(3)).getAsJsonObject();
				scopeID = scopeObject.get("id").getAsLong();
				scopeName = scopeObject.get("name").getAsString();
			}

			rdds.add(new RDD(row.getLong(0), parentList, row.getString(2),
					scopeID, scopeName, row.getLong(4)));

		}
		return rdds;
	}

	/**
	 * builds the graph with RDD dependencies and saves it into a .dot file that
	 * can be used for visualization, starting from a list of RDDs, This
	 * information include RDD ID, and names
	 * 
	 * @param rdds
	 * @throws IOException
	 */
	private static void printRDDGraph(List<RDD> rdds) throws IOException {

		DirectedAcyclicGraph<RDD, DefaultEdge> dag = new DirectedAcyclicGraph<RDD, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for rdds quickly
		// add vertexes to the graph
		HashMap<Long, RDD> rddMap = new HashMap<Long, RDD>(rdds.size());
		for (RDD rdd : rdds) {
			rddMap.put(rdd.getId(), rdd);
			dag.addVertex(rdd);
			logger.info("Added RDD" + rdd.getId() + " to the graph");
		}

		// add all edges then
		for (RDD rdd : rdds) {
			if (rdd.getParentIDs() != null)
				for (Long source : rdd.getParentIDs()) {
					dag.addEdge(rddMap.get(source), rdd);
					logger.info("Added link from RDD " + source + "to RDD"
							+ rdd.getId());
				}
		}

		DOTExporter<RDD, DefaultEdge> exporter = new DOTExporter<RDD, DefaultEdge>(
				new VertexNameProvider<RDD>() {

					public String getVertexName(RDD rdd) {
						return RDD_LABEL + rdd.getId();
					}
				}, new VertexNameProvider<RDD>() {

					public String getVertexName(RDD rdd) {
						return rdd.getName() + " (" + rdd.getId() + ")";
					}
				}, null);

		OutputStream os = hdfs.create(new Path(config.outputFile,
				"rdd-graph.dot"));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();
	}

	/**
	 * builds the graph with stages dependencies and saves it into a .dot file
	 * that can be used for visualization, starting from a list of stages
	 * 
	 * @param stages
	 * @throws IOException
	 */
	private static void printStageGraph(List<Stage> stages) throws IOException {
		DirectedAcyclicGraph<Stage, DefaultEdge> dag = new DirectedAcyclicGraph<Stage, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for stages quickly
		// and add vertexes to the graph
		HashMap<Long, Stage> stageMap = new HashMap<Long, Stage>(stages.size());
		for (Stage stage : stages) {
			stageMap.put(stage.getId(), stage);
			dag.addVertex(stage);
			logger.info("Added Stage " + stage.getId() + " to the graph");
		}

		// add all edges then
		for (Stage stage : stages) {
			if (stage.getParentIDs() != null)
				for (Long source : stage.getParentIDs()) {
					dag.addEdge(stageMap.get(source), stage);
					logger.info("Added link from Stage " + source + "to Stage"
							+ stage.getId());
				}
		}

		DOTExporter<Stage, DefaultEdge> exporter = new DOTExporter<Stage, DefaultEdge>(
				new VertexNameProvider<Stage>() {

					public String getVertexName(Stage stage) {
						return JOB_LABEL + stage.getJobId() + STAGE_LABEL
								+ stage.getId();
					}
				}, new VertexNameProvider<Stage>() {

					public String getVertexName(Stage stage) {
						return JOB_LABEL + stage.getJobId() + " " + STAGE_LABEL
								+ stage.getId();
					}
				}, null);

		OutputStream os = hdfs.create(new Path(config.outputFile,
				"stage-graph.dot"));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();
	}

	/**
	 * builds the graph with RDD dependencies and saves it into a .dot file that
	 * can be used for visualization, starting from the stageDetails. this
	 * visualization includes only RDD IDs
	 * 
	 * @param stageDetails
	 * @throws IOException
	 */
	@Deprecated
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
			List<Long> sources = null;
			if (row.get(1) instanceof scala.collection.immutable.List<?>)
				sources = JavaConversions.asJavaList((Seq<Long>) row.get(1));
			else if (row.get(1) instanceof ArrayBuffer<?>)
				sources = JavaConversions.asJavaList((ArrayBuffer<Long>) row
						.get(1));
			else {
				logger.warn("Could not parse RDD PArent IDs Serialization:"
						+ row.get(1).toString() + " class: "
						+ row.get(1).getClass() + " Object: " + row.get(1));
			}
			if (sources != null)
				for (Long source : sources) {
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
	@Deprecated
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
			List<Long> sources = null;
			if (row.get(1) instanceof scala.collection.immutable.List<?>)
				sources = JavaConversions.asJavaList((Seq<Long>) row.get(1));
			else if (row.get(1) instanceof ArrayBuffer<?>)
				sources = JavaConversions.asJavaList((ArrayBuffer<Long>) row
						.get(1));
			else {
				logger.warn("Could not parse Stage Parent IDs Serialization:"
						+ row.get(1).toString() + " class: "
						+ row.get(1).getClass() + " Object: " + row.get(1));
			}

			if (sources != null)
				for (Long source : sources) {
					logger.info("Adding link from " + STAGE_LABEL + source
							+ "to " + STAGE_LABEL + row.getLong(0));
					dag.addEdge(STAGE_LABEL + source,
							STAGE_LABEL + row.getLong(0));

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

		// initialize the extendedJobInfos table with initial and final ids for
		// job stages
		initializeJobInfos();

		// merge the stages start and stage end tables with rdd table to get the
		// desired information
		sqlContext
				.sql("SELECT	`start.Stage Info.Stage ID` AS id,"
						+ "		`start.Stage Info.Parent IDs` AS parentIDs,"
						+ "		`start.Stage Info.Stage Name` AS name,"
						+ "		`start.Stage Info.Number of Tasks` AS numberOfTasks,"
						+ "		`finish.Stage Info.Submission Time` AS submissionTime,"
						+ "		`finish.Stage Info.Completion Time` AS completionTime,"
						+ "		`finish.Stage Info.Completion Time` - `finish.Stage Info.Submission Time` AS executionTime,"
						+ "		`rddInfo.RDD ID`,"
						+ "		`rddInfo.Scope` AS RDDScope,"
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
						+ "		ON `start.Stage Info.Stage ID`=`rddInfos.Stage ID`")
				.registerTempTable("stages");

		DataFrame stageDetails = sqlContext
				.sql("SELECT `extendedJobStartInfos.Job ID`,"
						+ "stages.* "
						+ "FROM stages "
						+ "JOIN extendedJobStartInfos "
						+ "ON stages.id >= extendedJobStartInfos.minStageID AND stages.id <= extendedJobStartInfos.maxStageID");
		// jobDetails.show((int) jobDetails.count());

		return stageDetails;

	}

	/**
	 * Initialized the extendedJobStartInfos by selecting all the events that
	 * end with "Jobstart" label and adding two columns containing the m inimum
	 * and maximum stage id numbers for the job
	 */
	private static void initializeJobInfos() {
		// register the job submission event as a table, we will use it later to
		// divide stages into jobs
		// 1) select all the JobStart Events
		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%JobStart'")
				.registerTempTable("jobStartInfos");

		// 2a) create a temporary table expanding the Stage IDs column (it
		// contains an array)
		sqlContext
				.sql("SELECT `Job ID`, "
						+ "expandedStageIds "
						+ "FROM jobStartInfos LATERAL VIEW explode(`Stage IDs`) stageInfoTable AS expandedStageIds")
				.registerTempTable("JobID2StageID");
		// 2b) get the maximum and minimum from the expanded table
		// could not do in one query because hive does not support
		// min(explode())) notation
		sqlContext.sql(
				"SELECT `Job ID`, " + "MIN(expandedStageIds) AS minStageID, "
						+ "MAX(expandedStageIds) maxStageID "
						+ "FROM JobID2StageID " + "GROUP BY `Job ID`")
				.registerTempTable("JobStageBoundaries");
		// 3) Add the min and max columns to the start job table
		sqlContext
				.sql("SELECT jobStartInfos.*, "
						+ "JobStageBoundaries.minStageID,"
						+ "JobStageBoundaries.maxStageID "
						+ "FROM jobStartInfos "
						+ "JOIN JobStageBoundaries "
						+ "ON `jobStartInfos.Job ID`=`JobStageBoundaries.Job ID`")
				.registerTempTable("extendedJobStartInfos");
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
				if (row.get(i) instanceof String
						&& row.getString(i).startsWith("{")) {
					JsonParser parser = new JsonParser();
					JsonObject jsonObject = parser.parse(row.getString(i))
							.getAsJsonObject();
					for (Entry<String, JsonElement> element : jsonObject
							.entrySet())
						br.write(element.getValue().getAsString() + " ");
					br.write(",");
				} else if (row.get(i) instanceof ArrayBuffer<?>)
					br.write(((ArrayBuffer<?>) row.get(i)).mkString(" ") + ',');
				else
					br.write(row.get(i) + ",");
			}
			br.write("\n");
		}
		br.close();
	}

}
