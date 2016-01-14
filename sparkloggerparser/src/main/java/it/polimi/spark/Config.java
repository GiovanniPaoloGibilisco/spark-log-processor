package it.polimi.spark;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Config implements Serializable {

	/**
	 * generated serial version UID
	 */
	private static final long serialVersionUID = 5087417577620830639L;

	private static Config _instance;
	private static JCommander commander;

	static final Logger logger = LoggerFactory.getLogger(Config.class);

	private Config() {
	}

	public static Config getInstance() {
		if (_instance == null) {
			_instance = new Config();
		}
		return _instance;
	}

	public static void init(String[] args) {
		_instance = new Config();
		commander = new JCommander(_instance, args);
		logger.info(
				"Configuration: --inputFile {} --outputFolder {} --runlocal {} --appID {} --applicationDAG {} --jobDAGs {} "
						+ "--executedStages {} --stageRdds {} --jobRdds {} --task {} --usage {} "
						+ "--export {} --exportToDatabase {} --dbUser {} --dbPassword {} --dbUrl {}",
				new Object[] { _instance.inputFile, _instance.outputFolder,
						_instance.runLocal, _instance.applicationID,
						_instance.ApplicationDAG, _instance.jobDAGS,
						_instance.filterExecutedStages,
						_instance.buildStageRDDGraph,
						_instance.buildJobRDDGraph, _instance.task,
						_instance.usage, _instance.export, _instance.toDB,
						_instance.dbUser, _instance.dbPassword, _instance.dbUrl});
	}

	@Parameter(names = { "-i", "--inputFile" }, description = "Path to the input file containing the logs, the file must can be in the local file system or on hdfs. Either -i or -app has to be specified")
	public String inputFile;

	@Parameter(names = { "-o", "--outputFolder" }, required = true, description = "output folder to store the DAGs and csv files")
	public String outputFolder;

	@Parameter(names = { "-l", "--runLocal" }, description = "Use to run the tool in the local mode")
	public boolean runLocal = false;

	@Parameter(names = { "-app", "--appID" }, description = "Id of the application to analyze. If this parameter is specified (instead of using -i) the tool will look for log files using the configuration of the cluster")
	public String applicationID;

	@Parameter(names = { "-a", "--applicationDAG" }, description = "build a single DAG with all the stages created by the application")
	public boolean ApplicationDAG = false;

	@Parameter(names = { "-sr", "--stageRdds" }, description = "build a DAG for each stage  contaninng of all the RDDs of the stage")
	public boolean buildStageRDDGraph = false;

	@Parameter(names = { "-jr", "--jobRdds" }, description = "bbuild a DAG for each job contaninng of all the RDDs of the Job")
	public boolean buildJobRDDGraph = false;

	@Parameter(names = { "-t", "--task" }, description = "export performance information of tasks in csv")
	public boolean task = false;

	@Parameter(names = { "-j", "--jobDAGs" }, description = "build a separate DAG for each job")
	public boolean jobDAGS = false;

	@Parameter(names = { "-es", "--executedStages" }, description = "in the creation of DAGs (either with -a or -j) highlight stages that have been executed")
	public boolean filterExecutedStages = true;
	
	@Parameter(names = { "-er", "--executedRDDs" }, description = "avoid considering RDDs that have not been actually computed")
	public boolean filterComputedRDDs = true;

	@Parameter(names = { "-u", "--usage" }, description = "print this information screen")
	public boolean usage = false;

	@Parameter(names = { "-e", "--exportDag" }, description = "serializes all produced DAGs as java objects")
	public boolean export = false;

	@Parameter(names = { "--exportToDatabase" }, description = "Wether to export the results to the Database")
	public boolean toDB = false;

	@Parameter(names = { "--dbUser" }, description = "Username of the Database to which export the results")
	public String dbUser;

	@Parameter(names = { "--dbPassword" }, description = "Password of the Database to which export the results")
	public String dbPassword;

	@Parameter(names = { "--dbUrl" }, description = "Url of the Database to which export the results")
	public String dbUrl= "jdbc:mysql://minli39.sl.cloud9.ibm.com/SparkBench";	
	

	public void usage() {
		StringBuilder builder = new StringBuilder();
		commander.usage(builder);
		logger.info(builder.toString());
	}

}
