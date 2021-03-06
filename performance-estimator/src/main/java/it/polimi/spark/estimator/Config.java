package it.polimi.spark.estimator;

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
				"Configuration: --batchFolder {} --trainFraction{} --dagInputFolder {} --outputFolder {} --usage {} --stagePerformance {} --jobPerformance {} --exportToDatabase {} --dbUser {} --dbPassword {} --dbUrl {} --clusterName {} -- appId {} --test {}",
				new Object[] { _instance.benchmarkFolder, _instance.trainFraction, _instance.dagInputFolder, _instance.outputFile,
						_instance.usage, _instance.stagePerformanceFile,
						_instance.jobPerformanceFile, _instance.toDB,
						_instance.dbUser, _instance.dbPassword,
						_instance.dbUrl, _instance.clusterName, _instance.appId, _instance.testType  });
	}

	@Parameter(names = { "-b", "--batchFolder" }, description = "Path to the input folder containing the benchmarks")
	public String benchmarkFolder;	
	
	@Parameter(names = { "-t", "--trainFraction" }, description = "fraction of the applications in the input folder to use as training set")
	public double trainFraction;
	
	@Parameter(names = { "-i", "--dagInputFolder" }, description = "Path to the input folder containing the serialized DAGs")
	public String dagInputFolder;

	@Parameter(names = { "-o", "--outputFolder" },  description = "output folder to store something")
	public String outputFile;

	@Parameter(names = { "-s", "--stagePerformance" }, description = "Path to the stage performance CSV input file")
	public String stagePerformanceFile;

	@Parameter(names = { "-j", "--jobPerformance" }, description = "Path to the job performance CSV input file")
	public String jobPerformanceFile;

	@Parameter(names = { "-u", "--usage" }, description = "print this information screen")
	public boolean usage = false;

	@Parameter(names = { "--exportToDatabase" }, description = "Wether to export the results to the Database")
	public boolean toDB = false;

	@Parameter(names = { "--dbUser" }, description = "Username of the Database to which export the results")
	public String dbUser;

	@Parameter(names = { "--dbPassword" }, description = "Password of the Database to which export the results")
	public String dbPassword;

	@Parameter(names = { "--dbUrl" }, description = "Url of the Database to which export the results")
	public String dbUrl = "jdbc:mysql://minli39.sl.cloud9.ibm.com/SparkBench";

	@Parameter(names = { "--clusterName" }, description = "the name of the cluster (to be used as key in the database)")
	public String clusterName = null;

	@Parameter(names = { "--appId" }, description = "the Id of the application (to be used as key in the database)")
	public String appId = null;
	

	@Parameter(names = { "--test" }, description = "If performing a test run, the type of test (aggregation or estimation)")
	public String testType= "none";	
	
	public boolean batch=false;
	
	public boolean isBatch() {
		return batch;
	}

	public void usage() {
		StringBuilder builder = new StringBuilder();
		commander.usage(builder);
		logger.info(builder.toString());
	}

}
