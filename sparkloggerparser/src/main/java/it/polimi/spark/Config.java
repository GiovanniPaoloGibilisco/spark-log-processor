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
		new JCommander(_instance, args);
		logger.debug(
				"Configuration: --inputFile {} --outputFile {} --runlocal {} --appID",
				new Object[] { _instance.inputFile, _instance.outputFile, _instance.runLocal, _instance.applicationID});
	}

	@Parameter(names = { "-i", "--inputFile" })
	public String inputFile;

	@Parameter(names = { "-o", "--outputFile" }, required = true)
	public String outputFile;
	
	@Parameter(names = { "-l", "--runLocal" })
	public boolean runLocal = false;
	
	@Parameter(names = { "-a", "--appID" })
	public String applicationID;

}
