package it.polimi.spark.events;

public class TaskInfo {
	private int taskID;
	private int index;
	private int attempt;
	private long launchTime;
	private String executorID;
	private String host;
	private String locality;
	private boolean speculative;
	private long gettingResultTime;
	private long finishTime;
	private boolean failed;

	// TODO: find out what the accumulables tag in the log means
	public int getTaskID() {
		return taskID;
	}

	public void setTaskID(int taskID) {
		this.taskID = taskID;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getAttempt() {
		return attempt;
	}

	public void setAttempt(int attempt) {
		this.attempt = attempt;
	}

	public long getLaunchTime() {
		return launchTime;
	}

	public void setLaunchTime(long launchTime) {
		this.launchTime = launchTime;
	}

	public String getExecutorID() {
		return executorID;
	}

	public void setExecutorID(String executorID) {
		this.executorID = executorID;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getLocality() {
		return locality;
	}

	public void setLocality(String locality) {
		this.locality = locality;
	}

	public boolean isSpeculative() {
		return speculative;
	}

	public void setSpeculative(boolean speculative) {
		this.speculative = speculative;
	}

	public long getGettingResultTime() {
		return gettingResultTime;
	}

	public void setGettingResultTime(long gettingResultTime) {
		this.gettingResultTime = gettingResultTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public boolean isFailed() {
		return failed;
	}

	public void setFailed(boolean failed) {
		this.failed = failed;
	}

}
