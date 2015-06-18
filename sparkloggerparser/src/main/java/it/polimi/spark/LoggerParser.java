package it.polimi.spark;

import it.polimi.spark.dag.RDDnode;
import it.polimi.spark.dag.Stagenode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import scala.collection.Traversable;
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
	static final String APPLICATION_RDD_LABEL = "application-rdd";
	static final String DOT_EXTENSION = ".dot";
	static Map<Integer, List<Integer>> job2StagesMap = new HashMap<Integer, List<Integer>>();
	static Map<Integer, Integer> stage2jobMap = new HashMap<Integer, Integer>();

	@SuppressWarnings("deprecation")
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

		if (config.runLocal) {
			conf.setMaster("local[1]");
		}

		// either -i or -app has to be specified
		if (config.inputFile == null && config.applicationID == null) {
			logger.info("No input file (-i option) or application id (-a option) has been specified. At least one of these options has to be provided");
			config.usage();
			return;
		}

		// exactly one otherwise we will not know where the user wants to get
		// the logs from
		if (config.inputFile != null && config.applicationID != null) {
			logger.info("Either the input file (-i option) or the application id (-app option) has to be specified.");
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

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		sqlContext = new HiveContext(sc.sc());

		if (hdfs.exists(new Path(config.outputFolder)))
			hdfs.delete(new Path(config.outputFolder), true);

		// load the logs
		DataFrame logsframe = sqlContext.jsonFile(config.inputFile);
		logsframe = sqlContext.applySchema(logsframe.toJavaRDD(),
				(StructType) cleanSchema(logsframe.schema()));
		logsframe.cache();

		// register the main table with all the logs as "events"
		logsframe.registerTempTable("events");

		// stageDetails.show((int) stageDetails.count());

		// save CSV with performance information
		DataFrame taskDetails = null;
		if (config.task) {
			logger.info("Retrieving Task information");
			taskDetails = retrieveTaskInformation();
			saveListToCSV(taskDetails, "TaskDetails.csv");
		}

		DataFrame stageDetailsFrame = null;
		List<Row> stageDetails = null;
		List<String> stageDetailsColumns = null;

		DataFrame jobDetailsFrame = null;
		List<Row> jobDetails = null;
		List<String> jobDetailsColumns = null;

		List<Stagenode> stageNodes = null;
		List<Stage> stages = null;
		List<Job> jobs = null;

		int numberOfJobs = 0;
		int numberOfStages = 0;
		Benchmark application = null;

		DBHandler dbHandler = null;
		if (config.toDB) {
			if (config.dbUser == null)
				logger.warn("No user name has been specified for the connection with the DB, results will not be uploaded");
			if (config.dbPassword == null)
				logger.warn("No password has been specified for the connection with the DB, results will not be uploaded");
			if (config.dbUser != null && config.dbPassword != null) {
				dbHandler = new DBHandler(config.dbUrl, config.dbUser,
						config.dbPassword);
				logger.info("Retrieving Application Information");
				application = retrieveApplicationConfiguration();
			}
		}

		// I could also remove the if, almost every functionality require to
		// parse stage details
		if (config.ApplicationDAG || config.jobDAGS
				|| config.buildStageRDDGraph || config.buildJobRDDGraph
				|| config.toDB) {
			DataFrame applicationEvents = retrieveApplicationEvents();
			saveListToCSV(applicationEvents, "application.csv");
			// add the duration to the application
			if (application != null && config.toDB)
				application.setDuration(getDuration(applicationEvents));

			// collect stages from log files
			logger.info("Retrieving Stage Information");
			stageDetailsFrame = retrieveStageInformation();
			saveListToCSV(stageDetailsFrame, "StageDetails.csv");
			stageDetails = stageDetailsFrame.collectAsList();
			stageDetailsColumns = new ArrayList<String>(
					Arrays.asList(stageDetailsFrame.columns()));

			// collect jobs from log files
			logger.info("Retrieving Job Information");
			jobDetailsFrame = retrieveJobInformation();
			saveListToCSV(jobDetailsFrame, "JobDetails.csv");
			jobDetails = jobDetailsFrame.collectAsList();
			jobDetailsColumns = new ArrayList<String>(
					Arrays.asList(jobDetailsFrame.columns()));

			initMaps(stageDetails, stageDetailsColumns, jobDetails,
					jobDetailsColumns);

			stageNodes = extractStageNodes(stageDetails, stageDetailsColumns);
			if (config.toDB && application != null) {
				stages = extractStages(stageDetails, stageDetailsColumns,
						application.getClusterName(), application.getAppID());
				jobs = extractJobs(jobDetails, jobDetailsColumns,
						application.getClusterName(), application.getAppID());

				Map<Integer, Stage> stageById = new HashMap<Integer, Stage>(
						stages.size());

				for (Stage stage : stages)
					stageById.put(stage.getStageID(), stage);

				if (taskDetails != null)
					addStageSize(stageById, taskDetails);

				for (Job job : jobs) {
					// link job to application
					application.addJob(job);

					// link stages to jobs
					for (int stageId : job2StagesMap.get(job.getJobID()))
						// skip non executed
						if (stageById.containsKey(stageId))
							job.addStage(stageById.get(stageId));

				}

				int firstStageID = Integer.MAX_VALUE;
				for (int id : stageById.keySet())
					if (id < firstStageID)
						firstStageID = id;
				Stage firstStage = stageById.get(firstStageID);
				//convert MB to GB in the application
				application.setDataSize(firstStage.getInputSize()/1024);

			}

			numberOfJobs = jobDetails.size();
			numberOfStages = stageDetails.size();
		}

		if (config.ApplicationDAG) {
			logger.info("Building Stage DAG");
			DirectedAcyclicGraph<Stagenode, DefaultEdge> stageDag = buildStageDag(stageNodes);
			printStageGraph(stageDag);
		}

		if (config.jobDAGS) {
			for (int i = 0; i <= numberOfJobs; i++) {
				logger.info("Building Job DAG");
				DirectedAcyclicGraph<Stagenode, DefaultEdge> stageDag = buildStageDag(
						stageNodes, i);
				printStageGraph(stageDag, i);
				if (config.export)
					serializeDag(stageDag, JOB_LABEL + i);
			}
		}

		// This part takes care of functionalities related to rdds
		List<RDDnode> rdds = null;
		if (config.buildJobRDDGraph || config.buildStageRDDGraph) {
			logger.info("Building Retrieving RDD information");
			DataFrame rddDetails = retrieveRDDInformation();
			saveListToCSV(rddDetails, "rdd.csv");
			rdds = extractRDDs(rddDetails);
		}

		if (config.buildJobRDDGraph) {
			for (int i = 0; i <= numberOfJobs; i++) {
				logger.info("Building RDD Job DAG");
				DirectedAcyclicGraph<RDDnode, DefaultEdge> rddDag = buildRDDDag(
						rdds, i, -1);
				printRDDGraph(rddDag, i, -1);
				if (config.export)
					serializeDag(rddDag, RDD_LABEL + JOB_LABEL
							+ stage2jobMap.get(i).intValue());
			}
		}

		if (config.buildStageRDDGraph) {
			// register the current dataframe as jobs table
			logger.info("Building RDD Stage DAG");
			for (int i = 0; i <= numberOfStages; i++) {
				DirectedAcyclicGraph<RDDnode, DefaultEdge> rddDag = buildRDDDag(
						rdds, -1, i);
				printRDDGraph(rddDag, stage2jobMap.get(i).intValue(), i);
				if (config.export)
					serializeDag(rddDag, RDD_LABEL + JOB_LABEL
							+ stage2jobMap.get(i).intValue() + STAGE_LABEL + i);
			}

		}

		if (config.toDB && application != null && dbHandler != null) {
			logger.info("Adding application to the database");
			logger.info("Cluster name: " + application.getClusterName());
			logger.info("Application Id: " + application.getAppID());
			logger.info("Application Name: " + application.getAppName());			
			try {
				dbHandler.insertBenchmark(application);
			} catch (SQLException e) {
				logger.warn(
						"The application could not be added to the database ",
						e);
			} finally {
				saveApplicationInfo(application.getClusterName(),
						application.getAppID(), application.getAppName(),
						config.dbUser, application.getDataSize());
			}
		}
		if (dbHandler != null)
			dbHandler.close();
	}

	private static void addStageSize(Map<Integer, Stage> stageById,
			DataFrame taskDetails) {
		taskDetails.registerTempTable("tmp");

		DataFrame stageSizes = sqlContext
				.sql("SELECT stageID,"
						+ "sum(resultSize) as outputSize,"
						+ "sum(shuffleBytesWritten) as shuffleWriteSize,"
						+ "sum(bytesRead) as inputSize,"
						+ "sum(shuffleRemoteBytesRead) + sum(shuffleLocalBytesRead) as shuffleReadSize"
						+ " FROM tmp" + " GROUP BY stageID");

		ArrayList<String> stageSizeColumns = new ArrayList<String>(
				Arrays.asList(stageSizes.columns()));
		//we will use this to convert bytes (in the tables) to MBytes
		float byteToMByteFactor = 1/(1024*2);
		for (Row row : stageSizes.collectAsList()) {
			int stageID = (int) row
					.getLong(stageSizeColumns.indexOf("stageID"));
			Stage stage = stageById.get(stageID);
			if (!row.isNullAt(stageSizeColumns.indexOf("inputSize")))
				stage.setInputSize((double) row.getLong(stageSizeColumns
						.indexOf("inputSize"))*byteToMByteFactor);
			if (!row.isNullAt(stageSizeColumns.indexOf("outputSize")))
				stage.setOutputSize((double) row.getLong(stageSizeColumns
						.indexOf("outputSize"))*byteToMByteFactor);
			if (!row.isNullAt(stageSizeColumns.indexOf("shuffleReadSize")))
				stage.setShuffleReadSize((double) row.getLong(stageSizeColumns
						.indexOf("shuffleReadSize"))*byteToMByteFactor);
			if (!row.isNullAt(stageSizeColumns.indexOf("shuffleWriteSize")))
				stage.setShuffleWriteSize((double) row.getLong(stageSizeColumns
						.indexOf("shuffleWriteSize"))*byteToMByteFactor);
		}

		return;

	}

	/**
	 * Extract all the jobs in objects containing performance information, no
	 * topological info is saved
	 * 
	 * @param jobDetails
	 * @param jobColumns
	 * @param clusterName
	 * @param applicationID
	 * @return
	 */
	private static List<Job> extractJobs(List<Row> jobDetails,
			List<String> jobColumns, String clusterName, String appID) {

		List<Job> jobs = new ArrayList<Job>();
		for (Row row : jobDetails) {
			int jobId = (int) row.getLong(jobColumns.indexOf("Job ID"));
			Job job = new Job(clusterName, appID, jobId);
			job.setDuration((int) row.getLong(jobColumns.indexOf("Duration")));
			jobs.add(job);
		}
		return jobs;

	}

	/**
	 * Initializes the hashmaps used to retrieve job id from stage and viceversa
	 * (a list of stage id from a job id)
	 * 
	 * @param stageDetails
	 * @param stageColumns
	 * @param jobDetails
	 * @param jobColumns
	 */
	@SuppressWarnings("unchecked")
	private static void initMaps(List<Row> stageDetails,
			List<String> stageColumns, List<Row> jobDetails,
			List<String> jobColumns) {

		// setup hashmap from job to list of stages ids and viceversa
		for (Row row : jobDetails) {
			int jobID = (int) row.getLong(jobColumns.indexOf("Job ID"));
			List<Long> tmpStageList = null;
			List<Integer> stageList = null;
			if (row.get(jobColumns.indexOf("Stage IDs")) instanceof scala.collection.immutable.List<?>)
				tmpStageList = JavaConversions.asJavaList((Seq<Long>) row
						.get(jobColumns.indexOf("Stage IDs")));
			else if (row.get(jobColumns.indexOf("Stage IDs")) instanceof ArrayBuffer<?>)
				tmpStageList = JavaConversions
						.asJavaList((ArrayBuffer<Long>) row.get(jobColumns
								.indexOf("Stage IDs")));
			else {
				logger.warn("Could not parse Stage IDs Serialization:"
						+ row.get(jobColumns.indexOf("Stage IDs")).toString()
						+ " class: "
						+ row.get(jobColumns.indexOf("Stage IDs")).getClass()
						+ " Object: "
						+ row.get(jobColumns.indexOf("Stage IDs")));
			}

			// convert it to integers
			stageList = new ArrayList<Integer>();
			for (Long stage : tmpStageList) {
				stageList.add(stage.intValue());
				// Initialize the hashmap StageID -> JobID
				stage2jobMap.put(stage.intValue(), jobID);
			}
			// and the one JobID -> List of stage IDs
			job2StagesMap.put(jobID, stageList);
		}
	}

	/**
	 * Extract all the stages in objects containing performance information, no
	 * topological info is saved
	 * 
	 * @param stageDetails
	 * @param stageColumns
	 * @param clusterName
	 * @param applicationID
	 * @return
	 */
	private static List<Stage> extractStages(List<Row> stageDetails,
			List<String> stageColumns, String clusterName, String applicationID) {

		List<Stage> stages = new ArrayList<Stage>();
		for (Row row : stageDetails) {
			// filter outnon executed stages
			if (Boolean.parseBoolean(row.getString(stageColumns
					.indexOf("Executed")))) {
				int stageId = (int) row.getLong(stageColumns
						.indexOf("Stage ID"));
				Stage stage = new Stage(clusterName, applicationID,
						stage2jobMap.get(stageId), stageId);
				stage.setDuration(Integer.parseInt(row.getString(stageColumns
						.indexOf("Duration"))));
				// TODO: add parsing of input/output and shuffle sizes
				stages.add(stage);
			}
		}
		return stages;

	}

	private static int getDuration(DataFrame applicationEvents) {

		Row[] events = applicationEvents.collect();
		long startTime = 0;
		long endTime = 0;
		for (Row event : events) {
			if (event.getString(0).equals("SparkListenerApplicationStart"))
				startTime = event.getLong(2);
			else
				endTime = event.getLong(2);
		}

		return (int) (endTime - startTime);
	}

	private static Benchmark retrieveApplicationConfiguration() {

		DataFrame sparkPropertiesDf = sqlContext
				.sql("SELECT `Spark Properties` FROM events WHERE Event LIKE '%EnvironmentUpdate'");
		StructType schema = (StructType) ((StructType) sparkPropertiesDf
				.schema()).fields()[0].dataType();

		List<String> columns = new ArrayList<String>();
		for (String column : schema.fieldNames())
			columns.add(column);

		String selectedColumns = "";
		for (String column : columns) {
			selectedColumns += "`Spark Properties." + column + "`" + ", ";
		}

		sparkPropertiesDf = sqlContext.sql("SELECT " + selectedColumns
				+ "`System Properties.sun-java-command`"
				+ " FROM events WHERE Event LIKE '%EnvironmentUpdate'");

		// there should only be one of such events
		Row row = sparkPropertiesDf.toJavaRDD().first();

		Benchmark application = new Benchmark(row.getString(columns
				.indexOf("spark-master")), row.getString(columns
				.indexOf("spark-app-id")));
		if (columns.contains("spark-app-name"))
			application.setAppName(row.getString(columns
					.indexOf("spark-app-name")));

		if (columns.contains("spark-driver-memory")) {
			String memory = row.getString(columns
					.indexOf("spark-driver-memory"));
			// removing g or m
			memory = memory.substring(0, memory.length() - 1);
			application.setDriverMemory(Double.parseDouble(memory));
		}

		if (columns.contains("spark-executor-memory")) {
			String memory = row.getString(columns
					.indexOf("spark-executor-memory"));
			// removing g or m
			memory = memory.substring(0, memory.length() - 1);
			application.setExecutorMemory(Double.parseDouble(memory));
		}

		if (columns.contains("spark-kryoserializer-buffer-max")) {
			String memory = row.getString(columns
					.indexOf("spark-kryoserializer-buffer-max"));
			// removing g or m
			memory = memory.substring(0, memory.length() - 1);
			application.setKryoMaxBuffer(Integer.parseInt(memory));
		}

		if (columns.contains("spark-rdd-compress"))
			application.setRddCompress(Boolean.parseBoolean(row
					.getString(columns.indexOf("spark-rdd-compress"))));

		if (columns.contains("spark-storage-memoryFraction"))
			application
					.setStorageMemoryFraction(Double.parseDouble(row
							.getString(columns
									.indexOf("spark-storage-memoryFraction"))));

		if (columns.contains("spark-shuffle-memoryFraction"))
			application
					.setShuffleMemoryFraction(Double.parseDouble(row
							.getString(columns
									.indexOf("spark-shuffle-memoryFraction"))));

		String storageLevel = row.getString(columns.size()).split(" ")[row
				.getString(columns.size()).split(" ").length - 1];
		if (storageLevel.equals("MEMORY_ONLY")
				|| storageLevel.equals("MEMORY_AND_DISK")
				|| storageLevel.equals("DISK_ONLY")
				|| storageLevel.equals("MEMORY_AND_DISK_SER")
				|| storageLevel.equals("MEMORY_ONLY_2")
				|| storageLevel.equals("MEMORY_AND_DISK_2")
				|| storageLevel.equals("OFF_HEAP")
				|| storageLevel.equals("MEMORY_ONLY_SER"))
			application.setStorageLevel(storageLevel);

		// sqlContext.sql("SELECT * FROM SystemProperties").show();

		return application;

	}

	/**
	 * fixes the schema derived from json by subsituting dots in names with -
	 * 
	 * @param dataType
	 * @return
	 */
	private static DataType cleanSchema(DataType dataType) {
		if (dataType instanceof StructType) {
			StructField[] fields = new StructField[((StructType) dataType)
					.fields().length];
			int i = 0;
			for (StructField field : ((StructType) dataType).fields()) {
				fields[i] = field.copy(field.name().replace('.', '-'),
						cleanSchema(field.dataType()), field.nullable(),
						field.metadata());
				i++;
			}
			return new StructType(fields);
		} else if (dataType instanceof ArrayType) {
			return new ArrayType(
					cleanSchema(((ArrayType) dataType).elementType()),
					((ArrayType) dataType).containsNull());
		} else
			return dataType;

	}

	private static DataFrame retrieveApplicationEvents() {
		return sqlContext
				.sql("SELECT Event, `App ID`, Timestamp FROM events WHERE Event LIKE '%ApplicationStart' OR Event LIKE '%ApplicationEnd'");
	}

	/**
	 * serializes the DAG for later use
	 * 
	 * @param dag
	 * @param string
	 *            - the name of the file in which serialize the DAG
	 * @throws IOException
	 */
	private static void serializeDag(DirectedAcyclicGraph<?, DefaultEdge> dag,
			String filename) throws IOException {

		OutputStream os = hdfs.create(new Path(new Path(config.outputFolder,
				"dags"), filename));
		ObjectOutputStream objectStream = new ObjectOutputStream(os);
		objectStream.writeObject(dag);
		objectStream.close();
		os.close();
	}

	/**
	 * collects the information the RDDs (id, name, parents, scope, number of
	 * partitions) by looking into the "jobs" table
	 * 
	 * @return
	 */
	private static DataFrame retrieveRDDInformation() {

		return sqlContext
				.sql("SELECT `Stage Info.Stage ID`, "
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
						+ " FROM events LATERAL VIEW explode(`Stage Info.RDD Info`) rddInfoTable AS RDDInfo"
						+ " WHERE Event LIKE '%StageCompleted'");

	}

	/**
	 * gets a list of stages from the dataframe
	 * 
	 * @param stageDetails
	 *            - The collected dataframe containing stages details
	 * @param stageDetailsColumns
	 *            - The columns of stageDetails
	 * @param jobDetails
	 *            - The collected dataframe containing jobs details
	 * @param jobDetailsColumns
	 *            - The columns of job Details
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static List<Stagenode> extractStageNodes(List<Row> stageDetails,
			List<String> stageColumns) {
		List<Stagenode> stages = new ArrayList<Stagenode>();

		for (Row row : stageDetails) {
			List<Long> tmpParentList = null;
			List<Integer> parentList = null;
			if (row.get(stageColumns.indexOf("Parent IDs")) instanceof scala.collection.immutable.List<?>)
				tmpParentList = JavaConversions.asJavaList((Seq<Long>) row
						.get(stageColumns.indexOf("Parent IDs")));
			else if (row.get(stageColumns.indexOf("Parent IDs")) instanceof ArrayBuffer<?>)
				tmpParentList = JavaConversions
						.asJavaList((ArrayBuffer<Long>) row.get(stageColumns
								.indexOf("Parent IDs")));
			else {
				logger.warn("Could not parse Stage Parent IDs Serialization:"
						+ row.get(stageColumns.indexOf("Parent IDs"))
								.toString()
						+ " class: "
						+ row.get(stageColumns.indexOf("Parent IDs"))
								.getClass() + " Object: "
						+ row.get(stageColumns.indexOf("Parent IDs")));
			}

			parentList = new ArrayList<Integer>();
			for (Long parent : tmpParentList)
				parentList.add(parent.intValue());
			Stagenode stage = null;

			int stageId = (int) row.getLong(stageColumns.indexOf("Stage ID"));

			stage = new Stagenode(stage2jobMap.get(stageId), stageId,
					parentList, row.getString(stageColumns
							.indexOf("Stage Name")), Boolean.parseBoolean(row
							.getString(stageColumns.indexOf("Executed"))));
			stages.add(stage);

		}

		logger.info(stages.size() + "Stages found");
		return stages;
	}

	/**
	 * Extracts a list of RDDs from the table
	 * 
	 * @param stageDetails
	 * @return list of RDDs
	 */
	@SuppressWarnings("unchecked")
	private static List<RDDnode> extractRDDs(DataFrame rddDetails) {
		List<RDDnode> rdds = new ArrayList<RDDnode>();

		DataFrame table = rddDetails.select("RDD ID", "Parent IDs", "Name",
				"Scope", "Number of Partitions", "Stage ID", "Use Disk",
				"Use Memory", "Use ExternalBlockStore", "Deserialized",
				"Replication").distinct();
		for (Row row : table.collectAsList()) {
			List<Long> tmpParentList = null;
			List<Integer> parentList = null;
			if (row.get(1) instanceof scala.collection.immutable.List<?>)
				tmpParentList = JavaConversions.asJavaList((Seq<Long>) row
						.get(1));
			else if (row.get(1) instanceof ArrayBuffer<?>)
				tmpParentList = JavaConversions
						.asJavaList((ArrayBuffer<Long>) row.get(1));
			else {
				logger.warn("Could not parse RDD PArent IDs Serialization:"
						+ row.get(1).toString() + " class: "
						+ row.get(1).getClass() + " Object: " + row.get(1));
			}
			parentList = new ArrayList<Integer>();
			for (Long parent : tmpParentList)
				parentList.add(parent.intValue());
			int scopeID = 0;
			String scopeName = null;
			if (row.get(3) != null && !row.getString(3).isEmpty()
					&& row.getString(3).startsWith("{")) {
				JsonObject scopeObject = new JsonParser().parse(
						row.getString(3)).getAsJsonObject();
				scopeID = scopeObject.get("id").getAsInt();
				scopeName = scopeObject.get("name").getAsString();
			}

			rdds.add(new RDDnode((int) row.getLong(0), row.getString(2),
					parentList, scopeID, (int) row.getLong(4), scopeName,
					(int) row.getLong(5), row.getBoolean(6), row.getBoolean(7),
					row.getBoolean(8), row.getBoolean(9), (int) row.getLong(10)));

		}
		return rdds;
	}

	/**
	 * saves the dag it into a .dot file that can be used for visualization
	 * 
	 * @param dag
	 * @throws IOException
	 */
	private static void printRDDGraph(
			DirectedAcyclicGraph<RDDnode, DefaultEdge> dag, int jobNumber,
			int stageNumber) throws IOException {

		DOTExporter<RDDnode, DefaultEdge> exporter = new DOTExporter<RDDnode, DefaultEdge>(
				new VertexNameProvider<RDDnode>() {

					public String getVertexName(RDDnode rdd) {
						return STAGE_LABEL + rdd.getStageID() + RDD_LABEL
								+ rdd.getId();
					}
				}, new VertexNameProvider<RDDnode>() {

					public String getVertexName(RDDnode rdd) {
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
				}, new ComponentAttributeProvider<RDDnode>() {

					@Override
					public Map<String, String> getComponentAttributes(
							RDDnode rdd) {
						Map<String, String> map = new LinkedHashMap<String, String>();
						if (rdd.isUseMemory()) {
							map.put("style", "filled");
							map.put("fillcolor", "red");
						}
						return map;
					}
				}, null);

		String filename = null;
		// if we are building a dag for the entire application
		if (stageNumber < 0 && jobNumber < 0)
			filename = APPLICATION_RDD_LABEL + DOT_EXTENSION;
		else {
			// otherwise build the filename using jobnumber and stage number
			filename = RDD_LABEL;
			if (jobNumber >= 0)
				filename += JOB_LABEL + jobNumber;
			if (stageNumber >= 0)
				filename += STAGE_LABEL + stageNumber;
			filename += DOT_EXTENSION;
		}

		OutputStream os = hdfs.create(new Path(new Path(config.outputFolder,
				"rdd"), filename));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();
	}

	/**
	 * Build the DAG with RDD as nodes and Parent relationship as edges. job and
	 * stage number canbe used to specify the context of the DAG
	 * 
	 * @param rdds
	 * @param jobNumber
	 *            - uses only RDDs of the specified job (specify negative values
	 *            to use RDDs from all the jobs)
	 * @param stageNumber
	 *            - uses only RDDs of the specified stage (specify negative
	 *            values to use RDDs from all the stages)
	 * @return
	 */
	private static DirectedAcyclicGraph<RDDnode, DefaultEdge> buildRDDDag(
			List<RDDnode> rdds, int jobNumber, int stageNumber) {

		DirectedAcyclicGraph<RDDnode, DefaultEdge> dag = new DirectedAcyclicGraph<RDDnode, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for rdds quickly
		// add vertexes to the graph
		HashMap<Integer, RDDnode> rddMap = new HashMap<Integer, RDDnode>(
				rdds.size());
		for (RDDnode rdd : rdds) {
			if ((stageNumber < 0 || rdd.getStageID() == stageNumber)
					&& (jobNumber < 0 || stage2jobMap.get(rdd.getStageID()) == jobNumber)) {
				if (!rddMap.containsKey(rdd.getId())) {
					rddMap.put(rdd.getId(), rdd);
					if (!dag.containsVertex(rdd))
						dag.addVertex(rdd);
					logger.debug("Added RDD" + rdd.getId()
							+ " to the graph of stage " + stageNumber);
				}
			}
		}

		// add all edges then
		// note that we are ignoring edges going outside of the context (stage
		// or job)
		for (RDDnode rdd : dag.vertexSet()) {
			if (rdd.getParentIDs() != null)
				for (Integer source : rdd.getParentIDs()) {
					if (dag.vertexSet().contains(rddMap.get(source))) {
						logger.debug("Adding link from RDD " + source
								+ "to RDD" + rdd.getId());
						RDDnode sourceRdd = rddMap.get(source);
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
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag)
			throws IOException {
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
			DirectedAcyclicGraph<Stagenode, DefaultEdge> dag, int jobNumber)
			throws IOException {

		DOTExporter<Stagenode, DefaultEdge> exporter = new DOTExporter<Stagenode, DefaultEdge>(
				new VertexNameProvider<Stagenode>() {
					public String getVertexName(Stagenode stage) {
						return JOB_LABEL + stage.getJobId() + STAGE_LABEL
								+ stage.getId();
					}
				}, new VertexNameProvider<Stagenode>() {

					public String getVertexName(Stagenode stage) {
						return JOB_LABEL + stage.getJobId() + " " + STAGE_LABEL
								+ stage.getId();
					}
				}, null, new ComponentAttributeProvider<Stagenode>() {

					@Override
					public Map<String, String> getComponentAttributes(
							Stagenode stage) {
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
						if (edge.getSource() instanceof Stagenode
								&& ((Stagenode) edge.getSource()).isExecuted())
							map.put("color", "red");
						if (edge.getTarget() instanceof Stagenode
								&& ((Stagenode) edge.getTarget()).isExecuted())
							map.put("color", "red");
						return map;
					}
				});
		String filename = null;
		if (jobNumber < 0)
			filename = APPLICATION_DAG_LABEL + DOT_EXTENSION;
		else
			filename = JOB_LABEL + jobNumber + DOT_EXTENSION;
		OutputStream os = hdfs.create(new Path(new Path(config.outputFolder,
				"stage"), filename));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		exporter.export(br, dag);
		br.close();

	}

	/**
	 * Retireves a Dataframe with the following columns: Stage ID, Stage Name,
	 * Parent IDs, Submission Time, Completion Time, Duration, Number of Tasks,
	 * Executed
	 * */
	private static DataFrame retrieveStageInformation()
			throws UnsupportedEncodingException, IOException {

		sqlContext
				.sql("SELECT `Stage Info.Stage ID`,"
						+ "`Stage Info.Stage Name`,"
						+ "`Stage Info.Parent IDs`,"
						+ "`Stage Info.Number of Tasks`,"
						+ "`Stage Info.Submission Time`,"
						+ "`Stage Info.Completion Time`,"
						+ "`Stage Info.Completion Time` - `Stage Info.Submission Time` as Duration,"
						+ "'True' as Executed"
						+ " FROM events WHERE Event LIKE '%StageCompleted'")
				.registerTempTable("ExecutedStages");

		sqlContext
				.sql("SELECT `StageInfo.Stage ID`,"
						+ "`StageInfo.Stage Name`,"
						+ "`StageInfo.Parent IDs`,"
						+ "`StageInfo.Number of Tasks`,"
						+ "'0' as `Submission Time`,"
						+ "'0' as `Completion Time`,"
						+ "'0' as Duration,"
						+ "'False' as Executed"
						+ " FROM events LATERAL VIEW explode(`Stage Infos`) stageInfosTable AS `StageInfo`"
						+ " WHERE Event LIKE '%JobStart'").registerTempTable(
						"AllStages");

		sqlContext
				.sql("SELECT AllStages.* "
						+ "	FROM AllStages"
						+ "	LEFT JOIN ExecutedStages ON `AllStages.Stage ID`=`ExecutedStages.Stage ID`"
						+ "	WHERE `ExecutedStages.Stage ID` IS  NULL")
				.registerTempTable("NonExecutedStages");

		return sqlContext.sql("SELECT * " + "	FROM ExecutedStages"
				+ "	UNION ALL" + "	SELECT *" + "	FROM NonExecutedStages");

	}

	private static DataFrame retrieveJobInformation()
			throws UnsupportedEncodingException, IOException {

		return sqlContext
				.sql("SELECT `s.Job ID`,"
						+ "`s.Submission Time`,"
						+ "`e.Completion Time`,"
						+ "`e.Completion Time` - `s.Submission Time` as Duration,"
						+ "`s.Stage IDs`,"
						+ "max(prova) as maxStageID,"
						+ "min(prova) as minStageID"
						+ " FROM events s LATERAL VIEW explode(`s.Stage IDs`) idsTable as prova"
						+ "	INNER JOIN events e ON s.`Job ID`=e.`Job ID`"
						+ "	WHERE s.Event LIKE '%JobStart' AND e.Event LIKE '%JobEnd'"
						+ "GROUP BY `s.Job ID`,`s.Submission Time`,`e.Completion Time`,`s.Stage IDs`");

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
					// if the string starts with a parenthesis it is probably a
					// Json object not deserialized (the scope)
					JsonParser parser = new JsonParser();
					JsonObject jsonObject = parser.parse(row.getString(i))
							.getAsJsonObject();
					for (Entry<String, JsonElement> element : jsonObject
							.entrySet())
						br.write(element.getValue().getAsString() + " ");
					br.write(",");
				}
				// if it is an array print all the elements separated by a space
				// (instead of a comma)
				else if (row.get(i) instanceof Traversable<?>)
					br.write(((Traversable<?>) row.get(i)).mkString(" ") + ',');
				// if the element itself contains a comma then switch it to a
				// semicolon
				else if (row.get(i) instanceof String
						&& ((String) row.get(i)).contains(","))
					br.write(((String) row.get(i)).replace(',', ';') + ",");
				else {
					br.write(row.get(i) + ",");
				}
			}
			br.write("\n");
		}
		br.close();
	}

	private static void saveApplicationInfo(String clusterName, String appId,
			String appName, String dbUser, double dataSize) throws IOException {
		OutputStream os = hdfs.create(new Path(config.outputFolder,
				"application.info"));
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		br.write("Cluster name:" + clusterName + "\n");
		br.write("Application Id:" + appId + "\n");
		br.write("Application Name:" + appName + "\n");
		br.write("Database User:" + dbUser + "\n");
		br.write("Data Size:" + dataSize+ "\n");
		br.close();
	}

	/**
	 * Builds a DAG using all the stages in the provided list.
	 * 
	 * @param stages
	 * @return
	 */
	private static DirectedAcyclicGraph<Stagenode, DefaultEdge> buildStageDag(
			List<Stagenode> stages) {
		DirectedAcyclicGraph<Stagenode, DefaultEdge> dag = new DirectedAcyclicGraph<Stagenode, DefaultEdge>(
				DefaultEdge.class);

		// build an hashmap to look for stages quickly
		// and add vertexes to the graph
		HashMap<Integer, Stagenode> stageMap = new HashMap<Integer, Stagenode>(
				stages.size());
		for (Stagenode stage : stages) {
			stageMap.put(stage.getId(), stage);
			logger.debug("Adding Stage " + stage.getId() + " to the graph");
			dag.addVertex(stage);

		}

		// add all edges then
		for (Stagenode stage : stages) {
			if (stage.getParentIDs() != null)
				for (Integer source : stage.getParentIDs()) {
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
	private static DirectedAcyclicGraph<Stagenode, DefaultEdge> buildStageDag(
			List<Stagenode> stages, int jobNumber) {
		List<Stagenode> jobStages = new ArrayList<Stagenode>();
		for (Stagenode s : stages)
			if ((int) s.getJobId() == jobNumber)
				jobStages.add(s);
		return buildStageDag(jobStages);
	}

}
