package it.polimi.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stage {

	private String appID;
	private String appName;
	private String clusterName;
	private long duration;
	private double inputSize;
	private int jobID;
	private double outputSize;
	private double shuffleReadSize;
	private double shuffleWriteSize;
	private int numberOfTaks;
	public int getNumberOfTaks() {
		return numberOfTaks;
	}

	public void setNumberOfTaks(int numberOfTaks) {
		this.numberOfTaks = numberOfTaks;
	}

	private int id;
	static final Logger logger = LoggerFactory.getLogger(Stage.class);

	/**
	 * @param clustername
	 * @param appID
	 * @param jobID
	 * @param stageID
	 */
	public Stage(String clusterName, String appID, int jobID, int stageID) {
		super();
		this.clusterName = clusterName;
		this.appID = appID;
		this.jobID = jobID;
		this.id = stageID;
	}

	public String getAppID() {
		return appID;
	}

	public String getAppName() {
		return appName;
	}

	public String getClusterName() {
		return clusterName;
	}

	public long getDuration() {
		return duration;
	}

	public double getInputSize() {
		return inputSize;
	}

	public int getJobID() {
		return jobID;
	}

	public double getOutputSize() {
		return outputSize;
	}

	public double getShuffleReadSize() {
		return shuffleReadSize;
	}

	public double getShuffleWriteSize() {
		return shuffleWriteSize;
	}

	public int getID() {
		return id;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public void setInputSize(double inputSize) {
		this.inputSize = inputSize;
	}

	public void setOutputSize(double outputSize) {
		this.outputSize = outputSize;
	}

	public void setShuffleReadSize(double shuffleReadSize) {
		this.shuffleReadSize = shuffleReadSize;
	}

	public void setShuffleWriteSize(double shuffleWriteSize) {
		this.shuffleWriteSize = shuffleWriteSize;
	}

}
