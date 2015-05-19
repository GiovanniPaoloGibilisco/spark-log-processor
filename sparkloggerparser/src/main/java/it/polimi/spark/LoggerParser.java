package it.polimi.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.DefaultEdge;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.ext.ComponentAttributeProvider;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
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
	static final String APPLICATION_DAG_LABEL = "application-graph";
	static final String JOB_PREFIX_LABEL = "job";
	static final String GRAPH_LABEL = "-graph";
	static final String APPLICATION_RDD_LABEL = "application-rdd";
	static final String RDD_STAGE_PREFIX_LABEL = "rdd-stage";
	static final String DOT_EXTENSION = ".dot";

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

		if (config.usage) {
			config.usage();
			return;
		}

		if (config.runLocal)
			conf.setMaster("local[1]");

		// either -i or -app has to be specified
		if (config.inputFile == null && config.applicationID == null) {
			logger.info("No input file (-i option) or application id (-a option) has been specified. At least one of these options has to be provided");
			config.usage();
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
				logger.info("Could not retireve the logging directory from the spark configuration, the property spark.eventLog.dir has to be set in the cluster configuration");
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

		sqlContext = new HiveContext(sc.sc());

		if (hdfs.exists(new Path(config.outputFolder)))
			hdfs.delete(new Path(config.outputFolder), true);

		// load the logs
		DataFrame logsframe = sqlContext.jsonFile(config.inputFile);
		logsframe.cache();

		// register the main table with all the logs as "events"
		logsframe.registerTempTable("events");

		// stageDetails.show((int) stageDetails.count());

		// save CSV with performance information

		if (config.task) {
			DataFrame taskDetails = retrieveTaskInformation();
			saveListToCSV(taskDetails, "TaskDetails.csv");
		}

		DataFrame stageDetails = null;
		List<Stage> stages = null;

		if (config.ApplicationDAG || config.jobDAGS) {
			stageDetails = retrieveStageInformation();
			stages = extractStages(stageDetails);
			saveListToCSV(stageDetails, "StageDetails.csv");
		}

		if (config.ApplicationDAG) {
			DirectedAcyclicGraph<Stage, DefaultEdge> stageDag = buildStageDag(stages);

			printStageGraph(stageDag);
		}

		int numberOfJobs = 0;
		if (config.jobDAGS) {
			numberOfJobs = getNumberOfJobs(stageDetails);
			for (int i = 0; i <= numberOfJobs; i++) {
				DirectedAcyclicGraph<Stage, DefaultEdge> stageDag = buildStageDag(
						stages, i);
				printStageGraph(stageDag, i);
			}
		}

		// if(config.buildJobDags)

		if (config.buildRDDGraph) {
			// register the current dataframe as jobs table
			stageDetails.registerTempTable("jobs");
			DataFrame rddDetails = retrieveRDDInformation();
			// rddDetails.show();
			saveListToCSV(rddDetails, "rdd.csv");
			List<RDD> rdds = extractRDDs(rddDetails);

			int numberOfStages = getNumberOfStages(stageDetails);
			for (int i = 0; i <= numberOfStages; i++) {
				DirectedAcyclicGraph<RDD, DefaultEdge> rddDag = buildRDDDag(
						rdds, i);
				printRDDGraph(rddDag, i);
			}

		}

		// build images with dotty (if available)
		// check if dotty is available in the path
		try {
			if (DottyRenderer.isDottyAvailable() && filesAreLocal()) {
				if (config.ApplicationDAG) {
					new DottyRenderer(APPLICATION_DAG_LABEL + DOT_EXTENSION,
							APPLICATION_DAG_LABEL, "png").start();

				}
				if (config.jobDAGS) {
					for (int i = 0; i <= numberOfJobs; i++)
						new DottyRenderer(JOB_PREFIX_LABEL + i + GRAPH_LABEL
								+ DOT_EXTENSION, JOB_PREFIX_LABEL + i
								+ GRAPH_LABEL, "png").start();
				}
			} else {
				logger.info("could not find dot, DAGs have been exported but mages have not been rendered");
			}
		} catch (IOException e) {
			logger.info("could not run dotty, DAGs have been exported but mages have not been rendered");
		}

		// clean up the mess
		hdfs.close();
		sc.close();
	}

	/**
	 * check if thwe output folder is in the local file system
	 * 
	 * @return
	 */
	private static boolean filesAreLocal() {
		return !config.outputFolder.startsWith("hdfs");

	}

	/**
	 * gets the number of jobs in the application using the Job ID column
	 * 
	 * 
	 * @param stageDetails
	 * @return
	 */
	private static int getNumberOfJobs(DataFrame stageDetails) {
		int max = 0;
		for (Row r : stageDetails.select("Job ID").collect())
			if ((int) r.getLong(0) > max)
				max = (int) r.getLong(0);
		logger.info(max + " Jobs found");
		return max;
	}

	/**
	 * retrieves the number of stages (counting what's in the dataframe)
	 * 
	 * @param stageDetails
	 * @return
	 */
	private static int getNumberOfStages(DataFrame stageDetails) {
		return (int) stageDetails.count();
	}

	/**
	 * collects the information the RDDs (id, name, parents, scope, number of
	 * partitions) by looking into the "jobs" table
	 * 
	 * @return
	 */
	private static DataFrame retrieveRDDInformation() {

		DataFrame rdd = sqlContext
				.sql("SELECT `stageinfo.Stage ID`, "
						+ "`RDDInfo.RDD ID`,"
						+ "RDDInfo.Name,"
						+ "RDDInfo.Scope,"
						+ "`RDDInfo.Parent IDs`,"
						+ "`RDDInfo.Storage Level.Use Disk`,"
						+ "`RDDInfo.Storage Level.Use Memory`,"
						+ "`RDDInfo.Storage Level.Use ExternalBlockStore`,"
						+ "`RDDInfo.Storage Level.Deserialized`,"
						+ "`RDDInfo.Storage Level.Replication`,"
						+ "`RDDInfo.Number of Partitions`,"
						+ "`RDDInfo.Number of Cached Partitions`,"
						+ "`RDDInfo.Memory Size`,"
						+ "`RDDInfo.ExternalBlockStore Size`,"
						+ "`RDDInfo.Disk Size`"
						+ "FROM jobs LATERAL VIEW explode(`stageinfo.RDD Info`) rddInfoTable AS RDDInfo");

		return rdd;
	}

	/**
	 * gets a list of stages from the dataframe
	 * 
	 * @param stageDetails
	 * @return
	 */
	private static List<Stage> extractStages(DataFrame stageDetails) {
		List<Stage> stages = new ArrayList<Stage>();
		DataFrame table = null;
		if (config.filterExecutedStages)
			table = stageDetails.select("Job ID", "Stage ID", "Parent IDs",
					"Stage Name", "computed").orderBy("Job ID", "Stage ID");
		else
			table = stageDetails.select("Job ID", "Stage ID", "Parent IDs",
					"Stage Name").orderBy("Job ID", "Stage ID");

		for (Row row : table.distinct().collectAsList()) {
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

			Stage stage = null;
			if (config.filterExecutedStages)
				stage = new Stage(row.getLong(0), row.getLong(1), parentList,
						row.getString(3), row.getBoolean(4));
			else
				stage = new Stage(row.getLong(0), row.getLong(1), parentList,
						row.getString(3), false);

			stages.add(stage);

		}

		logger.info(stages.size() + "Stagess found");
		return stages;
	}

	/**
	 * Extracts a list of RDDs from the table
	 * 
	 * @param stageDetails
	 * @return list of RDDs
	 */
	private static List<RDD> extractRDDs(DataFrame rddDetails) {
		List<RDD> rdds = new ArrayList<RDD>();

		DataFrame table = rddDetails.select("RDD ID", "Parent IDs", "Name",
				"Scope", "Number of Partitions", "Stage ID", "Use Disk",
				"Use Memory", "Use ExternalBlockStore", "Deserialized",
				"Replication").distinct();
		for (Row row : table.collectAsList()) {
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

			rdds.add(new RDD(row.getLong(0), row.getString(2), parentList,
					scopeID, row.getLong(4), scopeName, row.getLong(5), row
							.getBoolean(6), row.getBoolean(7), row
							.getBoolean(8), row.getBoolean(9), row.getLong(10)));

		}
		return rdds;
	}

	/**
	 * convenience method to print all the rdd graph
	 * 
	 * @param dag
	 * @throws IOException
	 */
	private static void printRDDGraph(DirectedAcyclicGraph<RDD, DefaultEdge> dag)
			throws IOException {
		printRDDGraph(dag, -1);
	}

	/**
	 * saves the dag it into a .dot file that can be used for visualization
	 * 
	 * @param dag
	 * @throws IOException
	 */
	private static void printRDDGraph(
			DirectedAcyclicGraph<RDD, DefaultEdge> dag, int stage)
			throws IOException {

		DOTExporter<RDD, DefaultEdge> exporter = new DOTExporter<RDD, DefaultEdge>(
				new VertexNameProvider<RDD>() {

					public String getVertexName(RDD rdd) {
						return STAGE_LABEL + rdd.getStageID() + RDD_LABEL
								+ rdd.getId();
					}
				}, new VertexNameProvider<RDD>() {

					public String getVertexName(RDD rdd) {
						return rdd.getName() + " (" + rdd.getStageID() + ","
								+ rdd.getId() + ") ";
					}
				}, new EdgeNameProvider<DefaultEdge>() {

					@Override
					public String getEdgeName(DefaultEdge edge) {
						AttributeMap attributes = edge.getAttributes();
						if (attributes != null)
							if ((int) attributes.get("cardinality") > 1)
								return attributes.get("cardinality").toString();

						return null;
					}
				}, new ComponentAttributeProvider<RDD>() {

					@Override
					public Map<String, String> getComponentAttributes(RDD rdd) {
						Map<String, String> map = new LinkedHashMap<String, String>();
						if (rdd.isUseMemory()) {
							map.put("style", "filled");
							map.put("fillcolor", "red");
						}
						return map;
					}
				}, null);

		String filename = null;
		if (stage < 0)
			filename = APPLICATION_RDD_LABEL + DOT_EXTENSION;
		else
			filename = RDD_STAGE_PREFIX_LABEL + stage + GRAPH_LABEL
					+ DOT_EXTENSION;

		OutputStream os = hdfs.create(new Path(config.outputFolder, filename));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();
	}

	/**
	 * shorthand to build the dag over all the stages
	 * 
	 * @param rdds
	 * @return
	 */
	private static DirectedAcyclicGraph<RDD, DefaultEdge> buildRDDDag(
			List<RDD> rdds) {
		return buildRDDDag(rdds, -1);
	}

	/**
	 * build the dag of rdds of one stage
	 * 
	 * @param rdds
	 * @return
	 */
	private static DirectedAcyclicGraph<RDD, DefaultEdge> buildRDDDag(
			List<RDD> rdds, int stage) {

		DirectedAcyclicGraph<RDD, DefaultEdge> dag = new DirectedAcyclicGraph<RDD, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for rdds quickly
		// add vertexes to the graph
		HashMap<Long, RDD> rddMap = new HashMap<Long, RDD>(rdds.size());
		for (RDD rdd : rdds) {
			if (rdd.getStageID() == stage) {
				rddMap.put(rdd.getId(), rdd);
				if (!dag.containsVertex(rdd))
					dag.addVertex(rdd);
				logger.debug("Added RDD" + rdd.getId()
						+ " to the graph of stage " + stage);
			}
		}

		// add all edges then
		// note that we are ingoring edges going to other stages
		for (RDD rdd : dag.vertexSet()) {
			if (rdd.getParentIDs() != null)
				for (Long source : rdd.getParentIDs()) {
					if (dag.vertexSet().contains(rddMap.get(source))) {
						logger.debug("Adding link from RDD " + source
								+ "to RDD" + rdd.getId());
						RDD sourceRdd = rddMap.get(source);
						if (!dag.containsEdge(sourceRdd, rdd)) {
							dag.addEdge(sourceRdd, rdd);
							Map<String, Integer> map = new LinkedHashMap<>();
							map.put("cardinality", 1);
							dag.getEdge(sourceRdd, rdd).setAttributes(
									new AttributeMap(map));
						} else {
							int cardinality = (int) dag.getEdge(sourceRdd, rdd)
									.getAttributes().get("cardinality");
							dag.getEdge(sourceRdd, rdd).getAttributes()
									.put("cardinality", cardinality + 1);
						}

					}
				}
		}
		return dag;
	}

	/**
	 * convenience method to print the entire application dag
	 * 
	 * @param dag
	 * @throws IOException
	 */
	private static void printStageGraph(
			DirectedAcyclicGraph<Stage, DefaultEdge> dag) throws IOException {
		printStageGraph(dag, -1);

	}

	/**
	 * Export the Dag in dotty the job number is used to name the dot file, if
	 * it is -1 the file is names application-graph
	 * 
	 * @param dag
	 * @param jobNumber
	 * @throws IOException
	 */
	private static void printStageGraph(
			DirectedAcyclicGraph<Stage, DefaultEdge> dag, int jobNumber)
			throws IOException {

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
				}, null, new ComponentAttributeProvider<Stage>() {

					@Override
					public Map<String, String> getComponentAttributes(
							Stage stage) {
						Map<String, String> map = new LinkedHashMap<String, String>();
						if (stage.isExecuted()) {
							map.put("style", "filled");
							map.put("fillcolor", "red");
						}
						return map;
					}
				}, new ComponentAttributeProvider<DefaultEdge>() {

					@Override
					public Map<String, String> getComponentAttributes(
							DefaultEdge edge) {
						Map<String, String> map = new LinkedHashMap<String, String>();
						if (edge.getSource() instanceof Stage
								&& ((Stage) edge.getSource()).isExecuted())
							map.put("color", "red");
						if (edge.getTarget() instanceof Stage
								&& ((Stage) edge.getTarget()).isExecuted())
							map.put("color", "red");
						return map;
					}
				});
		String filename = null;
		if (jobNumber < 0)
			filename = APPLICATION_DAG_LABEL + DOT_EXTENSION;
		else
			filename = JOB_PREFIX_LABEL + jobNumber + GRAPH_LABEL
					+ DOT_EXTENSION;
		OutputStream os = hdfs.create(new Path(config.outputFolder, filename));
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

		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%JobStart'")
				.registerTempTable("jobs");
		sqlContext.sql(
				"SELECT * FROM events WHERE Event LIKE '%StageCompleted'")
				.registerTempTable("stageComputed");

		sqlContext.sql("SELECT * FROM events WHERE Event LIKE '%JobEnd'")
				.registerTempTable("jobEnd");

		sqlContext.sql(
				"SELECT  `Job ID`," + "`Submission Time`," + "`Stage Infos`,"
						+ "`Stage IDs`" + "FROM jobs ").registerTempTable(
				"jobs");

		retrieveInitialAndFinalStage();

		DataFrame stageDetails = sqlContext
				.sql("SELECT  *,"
						+ "`StageInfo.Stage ID`,"
						+ "`StageInfo.Stage Name`,"
						+ "`StageInfo.Parent IDs`"
						+ "FROM jobs LATERAL VIEW explode(`Stage Infos`) stageInfosTable AS StageInfo");

		if (config.filterExecutedStages) {
			stageDetails.registerTempTable("jobs");
			// To get the stages actually computed in the jobs table DataFrame
			stageDetails = sqlContext
					.sql("SELECT jobs.*,"
							+ "`stageComputed.Stage Info.Completion Time`,"
							+ "CASE  "
							+ "WHEN `stageComputed.Stage Info.Completion Time` IS NOT NULL THEN true "
							+ "WHEN `stageComputed.Stage Info.Completion Time` IS NULL THEN false "
							+ "END AS computed "
							+ "FROM jobs "
							+ "LEFt JOIN stageComputed "
							+ "ON `stageComputed.Stage Info.Stage ID` = `jobs.Stage ID`");
		}

		return stageDetails;

	}

	/**
	 * Initialize the extendedJobStartInfos by selecting all the events that end
	 * with "Jobstart" label and adding two columns containing the m inimum and
	 * maximum stage id numbers for the job (operates modifying the "jobs"
	 * table)
	 */
	private static void retrieveInitialAndFinalStage() {
		// 1a) create a temporary table expanding the Stage IDs column (it
		// contains an array)
		sqlContext
				.sql("SELECT `Job ID`, "
						+ "expandedStageIds "
						+ "FROM jobs LATERAL VIEW explode(`Stage IDs`) stageInfoTable AS expandedStageIds")
				.registerTempTable("JobID2StageID");
		// 1b) get the maximum and minimum from the expanded table
		// could not do in one query because hive does not support
		// min(explode())) notation
		sqlContext.sql(
				"SELECT `Job ID`, " + "MIN(expandedStageIds) AS minStageID, "
						+ "MAX(expandedStageIds) maxStageID "
						+ "FROM JobID2StageID " + "GROUP BY `Job ID`")
				.registerTempTable("JobStageBoundaries");
		// 2) Add the min and max columns to the start job table
		sqlContext.sql(
				"SELECT jobs.*, " + "JobStageBoundaries.minStageID,"
						+ "JobStageBoundaries.maxStageID " + "FROM jobs "
						+ "JOIN JobStageBoundaries "
						+ "ON `jobs.Job ID`=`JobStageBoundaries.Job ID`")
				.registerTempTable("jobs");
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
		OutputStream os = hdfs.create(new Path(config.outputFolder, fileName));
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

	/**
	 * Builds a 2 level DAG where the fist level is composed by jobs and the
	 * second level is composed by stages (not sure if this is really useful..)
	 * 
	 * @param stages
	 * @return
	 */
	@Deprecated
	private static DirectedAcyclicGraph<DirectedAcyclicGraph<Stage, DefaultEdge>, DefaultEdge> buildApplicationDag(
			List<Stage> stages) {
		DirectedAcyclicGraph<DirectedAcyclicGraph<Stage, DefaultEdge>, DefaultEdge> dag = new DirectedAcyclicGraph<DirectedAcyclicGraph<Stage, DefaultEdge>, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for stages quickly
		// and add vertexes to the graph
		Map<Long, HashMap<Long, Stage>> jobStageMap = new HashMap<Long, HashMap<Long, Stage>>();
		Map<Long, DirectedAcyclicGraph<Stage, DefaultEdge>> jobDagMap = new HashMap<Long, DirectedAcyclicGraph<Stage, DefaultEdge>>();
		for (Stage stage : stages) {
			if (!jobStageMap.containsKey(stage.getJobId()))
				jobStageMap.put(stage.getJobId(), new HashMap<Long, Stage>());
			jobStageMap.get(stage.getJobId()).put(stage.getId(), stage);
		}

		for (Long job : jobStageMap.keySet()) {
			logger.debug("Adding job " + job + " to the graph");
			DirectedAcyclicGraph<Stage, DefaultEdge> jobDag = new DirectedAcyclicGraph<Stage, DefaultEdge>(
					DefaultEdge.class);
			jobDagMap.put(job, jobDag);
			// add vertixes to the job dag
			for (Long stageId : jobStageMap.get(job).keySet()) {
				logger.debug("Adding Stage " + stageId + " of Job " + job
						+ " to the graph");
				jobDag.addVertex(jobStageMap.get(job).get(stageId));
			}

			// add edges to the jobdag
			for (Long stageId : jobStageMap.get(job).keySet()) {
				Stage stage = jobStageMap.get(job).get(stageId);
				if (stage.getParentIDs() != null)
					for (Long source : stage.getParentIDs()) {
						logger.debug("Adding link from Stage " + source
								+ "to Stage" + stage.getId());
						jobDag.addEdge(jobStageMap.get(job).get(source), stage);
					}
			}
			dag.addVertex(jobDag);

			// connecte linearly the upper level
			if (jobStageMap.containsKey(job - 1.0))
				dag.addEdge(jobDagMap.get(job - 1.0), jobDag);
		}

		return dag;
	}

	/**
	 * Builds a DAG using all the stages in the provided list.
	 * 
	 * @param stages
	 * @return
	 */
	private static DirectedAcyclicGraph<Stage, DefaultEdge> buildStageDag(
			List<Stage> stages) {
		DirectedAcyclicGraph<Stage, DefaultEdge> dag = new DirectedAcyclicGraph<Stage, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for stages quickly
		// and add vertexes to the graph
		HashMap<Long, Stage> stageMap = new HashMap<Long, Stage>(stages.size());
		for (Stage stage : stages) {
			stageMap.put(stage.getId(), stage);
			logger.debug("Adding Stage " + stage.getId() + " to the graph");
			dag.addVertex(stage);

		}

		// add all edges then
		for (Stage stage : stages) {
			if (stage.getParentIDs() != null)
				for (Long source : stage.getParentIDs()) {
					logger.debug("Adding link from Stage " + source
							+ "to Stage" + stage.getId());
					dag.addEdge(stageMap.get(source), stage);
				}
		}
		return dag;
	}

	/**
	 * builds a DAG using only the stages in the specified job, selected by
	 * those provided in the list
	 * 
	 * @param stages
	 * @param stageNumber
	 * @return
	 */
	private static DirectedAcyclicGraph<Stage, DefaultEdge> buildStageDag(
			List<Stage> stages, int jobNumber) {
		List<Stage> jobStages = new ArrayList<Stage>();
		for (Stage s : stages)
			if ((int) s.getJobId() == jobNumber)
				jobStages.add(s);
		return buildStageDag(jobStages);
	}

}
