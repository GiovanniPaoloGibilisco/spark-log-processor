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
		logger.info("Configuration: --inputFolder {} --outputFolder {} --usage {}",
				new Object[] { _instance.inputFolder, _instance.outputFolder, _instance.usage });
	}

	@Parameter(names = { "-i", "--inputFolder" }, required = true, description = "Path to the input folder containing the serialized DAGs")
	public String inputFolder;

	@Parameter(names = { "-o", "--outputFolder" }, required = false, description = "output folder to store something")
	public String outputFolder;
	
	@Parameter(names = { "-u", "--usage" }, description = "print this information screen")
	public boolean usage = false;

	public void usage() {
		StringBuilder builder = new StringBuilder();
		commander.usage(builder);
		logger.info(builder.toString());
	}

}
