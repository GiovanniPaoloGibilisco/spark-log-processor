package it.polimi.spark;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Benchmark {

	private String appID;
	private String appName;
	private String clusterName;
	private double dataSize;
	private double driverMemory;
	private double executorMemory;
	private int executors;
	private int kryoMaxBuffer;
	private String logFolder;
	private int parallelism;
	private boolean rddCompress = false;
	private int duration;
	private double shuffleMemoryFraction;
	private String state;
	private String storageLevel;
	private double storageMemoryFraction;
	static final Logger logger = LoggerFactory.getLogger(Benchmark.class);
	private List<Job> jobs;

	public Benchmark(String clusterName, String appID) {
		this.clusterName = clusterName;
		this.appID = appID;
		jobs = new ArrayList<Job>();
	}
	
	public int getDuration() {
		return duration;
	}
	
	public void setDuration(int duration) {
		this.duration = duration;
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

	public double getDataSize() {
		return dataSize;
	}

	public double getDriverMemory() {
		return driverMemory;
	}

	public double getExecutorMemory() {
		return executorMemory;
	}

	public int getExecutors() {
		return executors;
	}

	public int getKryoMaxBuffer() {
		return kryoMaxBuffer;
	}

	public String getLogFolder() {
		return logFolder;
	}

	public int getParallelism() {
		return parallelism;
	}

	public double getShuffleMemoryFraction() {
		return shuffleMemoryFraction;
	}

	public String getState() {
		return state;
	}

	public String getStorageLevel() {
		return storageLevel;
	}

	public double getStorageMemoryFraction() {
		return storageMemoryFraction;
	}

	public boolean isRddCompress() {
		return rddCompress;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public void setDataSize(double dataSize) {
		this.dataSize = dataSize;
	}

	public void setDriverMemory(double driverMemory) {
		this.driverMemory = driverMemory;
	}

	public void setExecutorMemory(double executorMemory) {
		this.executorMemory = executorMemory;
	}

	public void setExecutors(int executors) {
		this.executors = executors;
	}

	public void setKryoMaxBuffer(int kryoMaxBuffer) {
		this.kryoMaxBuffer = kryoMaxBuffer;
	}

	public void setLogFolder(String logFolder) {
		this.logFolder = logFolder;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public void setRddCompress(boolean rddCompress) {
		this.rddCompress = rddCompress;
	}

	public void setShuffleMemoryFraction(double shuffleMemoryFraction) {
		this.shuffleMemoryFraction = shuffleMemoryFraction;
	}

	public void setState(String state) {
		this.state = state;
	}

	public void setStorageLevel(String storageLevel) {
		this.storageLevel = storageLevel;
	}

	public void setStorageMemoryFraction(double storageMemoryFraction) {
		this.storageMemoryFraction = storageMemoryFraction;
	}

	public void addJob(Job job) {
		if (job.getClusterName() != getClusterName()
				|| job.getAppID() != getAppID()) {
			logger.warn("Trying to add a job with wrong cluster name or application id, Skipped Job with id: "
					+ job.getJobID());
			return;
		}

		jobs.add(job);
	}

	public List<Job> getJobs() {
		return jobs;
	}
}
